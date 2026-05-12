import { buildStreamHandler } from './analyzeStream';
import { PluginContext, StreamChunk } from '../types';

interface MockWs {
  send: jest.Mock;
  close: jest.Mock;
  onmessage: ((event: { data: any }) => void) | null;
  onerror: ((event: any) => void) | null;
  onclose: ((event: any) => void) | null;
  onopen: ((event: any) => void) | null;
  sent: string[];
  pushFrame: (frame: any) => void;
}

const makeMockWs = (): MockWs => {
  const ws: any = {
    sent: [],
    send: jest.fn(function (this: any, data: string) {
      ws.sent.push(data);
    }),
    close: jest.fn(),
    onmessage: null,
    onerror: null,
    onclose: null,
    onopen: null,
    pushFrame(frame: any) {
      ws.onmessage?.({ data: JSON.stringify(frame) });
    },
  };
  return ws as MockWs;
};

const makeContext = (): PluginContext => ({
  request: {
    json: {
      messages: [{ role: 'user', content: 'hi' }],
    },
    headers: { 'x-portkey-trace-id': 'trace-1' },
  },
  response: { json: null },
  requestType: 'chatComplete',
  provider: 'openai',
  metadata: {},
});

const baseParams = {
  credentials: { apiKey: 'test-key' },
};

const chunk = (data: any): StreamChunk => ({
  raw: `data: ${JSON.stringify(data)}\n\n`,
  data,
  kind: 'data',
});
const doneChunk = (): StreamChunk => ({
  raw: 'data: [DONE]\n\n',
  kind: 'done',
});

async function* upstreamOf(chunks: StreamChunk[]): AsyncIterable<StreamChunk> {
  for (const c of chunks) yield c;
}

const collect = async (iter: AsyncIterable<StreamChunk>) => {
  const out: StreamChunk[] = [];
  for await (const c of iter) out.push(c);
  return out;
};

describe('Cato streaming guardrail', () => {
  it('forwards verified chunks from Cato and terminates on done', async () => {
    const ws = makeMockWs();
    const factory = jest.fn(async () => ws as any);
    const handler = buildStreamHandler(factory);

    const verifiedChunk = {
      id: 'c1',
      object: 'chat.completion.chunk',
      choices: [{ index: 0, delta: { content: 'hello' } }],
    };

    const upstream = upstreamOf([
      chunk({ id: 'c1', choices: [{ delta: { content: 'hello' } }] }),
      doneChunk(),
    ]);

    const iter = handler(makeContext(), baseParams, upstream);
    const collector = collect(iter);

    // simulate Cato responding with verified_chunk then done
    await new Promise((r) => setImmediate(r));
    ws.pushFrame({ verified_chunk: verifiedChunk });
    ws.pushFrame({ done: true });

    const out = await collector;
    expect(out).toHaveLength(2);
    expect(out[0].data).toEqual(verifiedChunk);
    expect(out[1].kind).toBe('done');
    expect(factory).toHaveBeenCalledTimes(1);
    expect(ws.sent[0]).toBe(
      JSON.stringify({ id: 'c1', choices: [{ delta: { content: 'hello' } }] })
    );
  });

  it('blocks the stream when Cato sends blocking_message', async () => {
    const ws = makeMockWs();
    const factory = jest.fn(async () => ws as any);
    const handler = buildStreamHandler(factory);

    const upstream = upstreamOf([
      chunk({ id: 'c1', choices: [{ delta: { content: 'leaking SSN' } }] }),
      doneChunk(),
    ]);

    const iter = handler(makeContext(), baseParams, upstream);
    const collector = collect(iter);

    await new Promise((r) => setImmediate(r));
    ws.pushFrame({
      blocking_message: 'Policy violation detected',
      first_blocked_chunk: {
        id: 'blocked-1',
        object: 'chat.completion.chunk',
      },
    });

    const out = await collector;
    expect(out).toHaveLength(2);
    expect(out[0].data.choices[0].delta.content).toContain(
      'Policy violation detected'
    );
    expect(out[0].data.choices[0].finish_reason).toBe('content_filter');
    expect(out[1].kind).toBe('done');
  });

  it('fails open and forwards upstream when Cato connection throws (failOpen=true)', async () => {
    const factory = jest.fn(async () => {
      throw new Error('ECONNREFUSED');
    });
    const handler = buildStreamHandler(factory);

    const upstream = upstreamOf([
      chunk({ delta: { content: 'a' } }),
      doneChunk(),
    ]);

    const out = await collect(handler(makeContext(), baseParams, upstream));
    expect(out).toHaveLength(2);
    expect(out[0].data.delta.content).toBe('a');
    expect(out[1].kind).toBe('done');
  });

  it('fails closed when Cato connection throws and failOpen=false', async () => {
    const factory = jest.fn(async () => {
      throw new Error('ECONNREFUSED');
    });
    const handler = buildStreamHandler(factory);

    const upstream = upstreamOf([
      chunk({ delta: { content: 'a' } }),
      doneChunk(),
    ]);

    const out = await collect(
      handler(makeContext(), { ...baseParams, failOpen: false }, upstream)
    );
    expect(out[0].data.choices[0].finish_reason).toBe('content_filter');
    expect(out[1].kind).toBe('done');
  });

  it('sends auth headers and call-id to the WS factory', async () => {
    const ws = makeMockWs();
    const factory = jest.fn(async () => ws as any);
    const handler = buildStreamHandler(factory);

    const upstream = upstreamOf([doneChunk()]);
    const iter = handler(
      makeContext(),
      { ...baseParams, userEmail: 'u@x.com', keyAlias: 'k1' },
      upstream
    );
    const collector = collect(iter);

    await new Promise((r) => setImmediate(r));
    ws.pushFrame({ done: true });
    await collector;

    const call = factory.mock.calls[0] as unknown as [
      string,
      Record<string, string>,
    ];
    expect(call[0]).toMatch(
      /^wss:\/\/api\.aisec\.catonetworks\.com\/fw\/v1\/analyze\/stream$/
    );
    expect(call[1].Authorization).toBe('Bearer test-key');
    expect(call[1]['x-cato-call-id']).toBe('trace-1');
    expect(call[1]['x-cato-user-email']).toBe('u@x.com');
    expect(call[1]['x-cato-gateway-key-alias']).toBe('k1');
  });

  it('forwards a {done:true} sentinel to Cato when upstream ends', async () => {
    const ws = makeMockWs();
    const factory = jest.fn(async () => ws as any);
    const handler = buildStreamHandler(factory);

    const upstream = upstreamOf([
      chunk({ delta: { content: 'a' } }),
      doneChunk(),
    ]);

    const iter = handler(makeContext(), baseParams, upstream);
    const collector = collect(iter);

    await new Promise((r) => setImmediate(r));
    ws.pushFrame({ verified_chunk: { delta: { content: 'a' } } });
    ws.pushFrame({ done: true });

    await collector;

    expect(ws.sent).toContain(JSON.stringify({ done: true }));
  });

  it('honors a custom apiBase by switching to wss://', async () => {
    const ws = makeMockWs();
    const factory = jest.fn(async () => ws as any);
    const handler = buildStreamHandler(factory);

    const upstream = upstreamOf([doneChunk()]);
    const iter = handler(
      makeContext(),
      {
        ...baseParams,
        credentials: {
          apiKey: 'test-key',
          apiBase: 'https://custom.example.com/',
        },
      },
      upstream
    );
    const collector = collect(iter);
    await new Promise((r) => setImmediate(r));
    ws.pushFrame({ done: true });
    await collector;

    const call = factory.mock.calls[0] as unknown as [
      string,
      Record<string, string>,
    ];
    expect(call[0]).toBe('wss://custom.example.com/fw/v1/analyze/stream');
  });
});
