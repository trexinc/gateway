import {
  PluginContext,
  PluginParameters,
  StreamChunk,
  StreamingPluginHandler,
  PluginStreamBlocked,
} from '../types';
import {
  CATO_ANALYZE_PATH,
  CATO_DEFAULT_BASE_URL,
  CATO_HOOK_VERSION,
} from './analyze';

const CATO_STREAM_PATH = `${CATO_ANALYZE_PATH}/stream`;

interface CatoCredentials {
  apiKey: string;
  apiBase?: string;
}

interface CatoStreamFrame {
  verified_chunk?: any;
  done?: boolean;
  blocking_message?: string;
  first_blocked_chunk?: any;
}

interface OpenWsLike {
  send(data: string): void;
  close(code?: number, reason?: string): void;
  onmessage: ((event: { data: any }) => void) | null;
  onerror: ((event: any) => void) | null;
  onclose: ((event: any) => void) | null;
  onopen: ((event: any) => void) | null;
}

export type WsFactory = (
  url: string,
  headers: Record<string, string>
) => Promise<OpenWsLike>;

const defaultWsFactory: WsFactory = async (url, headers) => {
  const { WebSocket } = await import('ws');
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(url, { headers });
    const adapter: OpenWsLike = {
      send: (data) => ws.send(data),
      close: (code, reason) => ws.close(code, reason),
      onmessage: null,
      onerror: null,
      onclose: null,
      onopen: null,
    };
    ws.on('open', () => {
      adapter.onopen?.({});
      resolve(adapter);
    });
    ws.on('message', (data: any) => {
      adapter.onmessage?.({
        data: typeof data === 'string' ? data : data.toString('utf-8'),
      });
    });
    ws.on('error', (err: any) => {
      adapter.onerror?.(err);
      reject(err);
    });
    ws.on('close', (code: number, reason: Buffer) => {
      adapter.onclose?.({ code, reason: reason?.toString() });
    });
  });
};

const wsUrl = (apiBase: string) =>
  `${apiBase
    .replace(/\/$/, '')
    .replace(/^http:\/\//, 'ws://')
    .replace(/^https:\/\//, 'wss://')}${CATO_STREAM_PATH}`;

const parseSseDataChunk = (raw: string): any | null => {
  for (const line of raw.split(/\r?\n/)) {
    const trimmed = line.trimStart();
    if (!trimmed.startsWith('data:')) continue;
    const payload = trimmed.slice(5).trimStart();
    if (!payload || payload === '[DONE]') return null;
    try {
      return JSON.parse(payload);
    } catch {
      return null;
    }
  }
  return null;
};

const wrapAsSseChunk = (data: any): StreamChunk => ({
  raw: `data: ${JSON.stringify(data)}\n\n`,
  data,
  kind: 'data',
});

const doneChunk = (): StreamChunk => ({
  raw: `data: [DONE]\n\n`,
  kind: 'done',
});

const buildBlockingChunk = (
  firstBlocked: any | undefined,
  message: string
): StreamChunk => {
  const base = firstBlocked || {
    id: `cato-blocked-${Date.now()}`,
    object: 'chat.completion.chunk',
  };
  const synthesized = {
    ...base,
    choices: [
      {
        index: 0,
        delta: { role: 'assistant', content: `\n\n**${message}**` },
        finish_reason: 'content_filter',
      },
    ],
  };
  return wrapAsSseChunk(synthesized);
};

interface AsyncQueue<T> {
  push(v: T): void;
  end(err?: Error): void;
  next(): Promise<{ value: T | undefined; done: boolean; error?: Error }>;
}

const makeQueue = <T>(): AsyncQueue<T> => {
  const values: T[] = [];
  const waiters: Array<(r: any) => void> = [];
  let ended = false;
  let error: Error | undefined;

  const settle = () => {
    while (waiters.length && (values.length || ended)) {
      const resolve = waiters.shift()!;
      if (values.length) {
        resolve({ value: values.shift(), done: false });
      } else {
        resolve({ value: undefined, done: true, error });
      }
    }
  };

  return {
    push(v: T) {
      values.push(v);
      settle();
    },
    end(err?: Error) {
      ended = true;
      error = err;
      settle();
    },
    next() {
      return new Promise((resolve) => {
        waiters.push(resolve);
        settle();
      });
    },
  };
};

export const buildStreamHandler = (
  wsFactory: WsFactory = defaultWsFactory
): StreamingPluginHandler => {
  return async function* streamHandler(
    context: PluginContext,
    parameters: PluginParameters,
    upstream: AsyncIterable<StreamChunk>
  ): AsyncIterable<StreamChunk> {
    const credentials = parameters.credentials as CatoCredentials | undefined;
    const apiKey = credentials?.apiKey || (process.env.CATO_API_KEY as string);
    const failOpen = parameters.failOpen !== false;

    if (!apiKey) {
      if (!failOpen) {
        yield buildBlockingChunk(undefined, 'Cato API key not configured');
        yield doneChunk();
        return;
      }
      for await (const chunk of upstream) yield chunk;
      return;
    }

    const apiBase =
      credentials?.apiBase ||
      (process.env.CATO_API_BASE as string) ||
      CATO_DEFAULT_BASE_URL;

    const callId =
      context?.request?.headers?.['x-portkey-trace-id'] ||
      context?.metadata?.traceId ||
      (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function'
        ? crypto.randomUUID()
        : `${Date.now()}-${Math.random().toString(36).slice(2)}`);

    const headers: Record<string, string> = {
      Authorization: `Bearer ${apiKey}`,
      'x-cato-portkey-version': CATO_HOOK_VERSION,
      'x-cato-call-id': String(callId),
    };
    if (parameters.userEmail) {
      headers['x-cato-user-email'] = String(parameters.userEmail);
    }
    if (parameters.keyAlias) {
      headers['x-cato-gateway-key-alias'] = String(parameters.keyAlias);
    }

    let ws: OpenWsLike;
    try {
      ws = await wsFactory(wsUrl(apiBase), headers);
    } catch (err) {
      if (!failOpen) {
        yield buildBlockingChunk(undefined, 'Cato stream connection failed');
        yield doneChunk();
        return;
      }
      for await (const chunk of upstream) yield chunk;
      return;
    }

    const incoming = makeQueue<CatoStreamFrame>();
    ws.onmessage = (event) => {
      try {
        incoming.push(JSON.parse(event.data));
      } catch (e: any) {
        incoming.end(e);
      }
    };
    ws.onerror = (err) => incoming.end(err);
    ws.onclose = () => incoming.end();

    let senderAborted = false;
    const sender = (async () => {
      try {
        for await (const chunk of upstream) {
          if (senderAborted) break;
          if (chunk.kind === 'done') {
            ws.send(JSON.stringify({ done: true }));
            continue;
          }
          const payload = chunk.data ?? parseSseDataChunk(chunk.raw);
          if (payload != null) {
            ws.send(JSON.stringify(payload));
          }
        }
        if (!senderAborted) ws.send(JSON.stringify({ done: true }));
      } catch (err: any) {
        incoming.end(err);
      }
    })();

    try {
      while (true) {
        const next = await incoming.next();
        if (next.done) {
          if (next.error && !failOpen) {
            throw new PluginStreamBlocked('Cato stream error');
          }
          break;
        }
        const frame = next.value as CatoStreamFrame;
        if (frame.verified_chunk) {
          yield wrapAsSseChunk(frame.verified_chunk);
        } else if (frame.blocking_message) {
          senderAborted = true;
          yield buildBlockingChunk(
            frame.first_blocked_chunk,
            frame.blocking_message
          );
          yield doneChunk();
          return;
        } else if (frame.done) {
          yield doneChunk();
          return;
        }
      }
    } finally {
      senderAborted = true;
      try {
        ws.close();
      } catch {}
      await sender.catch(() => {});
    }
  };
};

export const streamHandler: StreamingPluginHandler = buildStreamHandler();
