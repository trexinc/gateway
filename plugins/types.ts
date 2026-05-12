export interface PluginContext {
  [key: string]: any;
  requestType?: 'complete' | 'chatComplete' | 'embed' | 'messages';
  provider?: string;
  metadata?: Record<string, any>;
}

export interface PluginParameters<K = Record<string, string>> {
  [key: string]: any;
  credentials?: K;
}

export interface PluginHandlerResponse {
  error: any;
  verdict?: boolean;
  // The data object can be any JSON object or null.
  data?: any | null;
  transformedData?: any;
  transformed?: boolean;
}

export type HookEventType =
  | 'beforeRequestHook'
  | 'afterRequestHook'
  | 'streamingAfterRequestHook';

export type PluginHandler<P = Record<string, string>> = (
  context: PluginContext,
  parameters: PluginParameters<P>,
  eventType: HookEventType,
  options?: {
    env: Record<string, any>;
    getFromCacheByKey?: (key: string) => Promise<any>;
    putInCacheWithValue?: (key: string, value: any) => Promise<any>;
  }
) => Promise<PluginHandlerResponse>;

export interface StreamChunk {
  raw: string;
  data?: any;
  kind: 'data' | 'done' | 'event' | 'raw';
}

export class PluginStreamBlocked extends Error {
  blockedChunk?: StreamChunk;
  constructor(
    public reason: string,
    blockedChunk?: StreamChunk
  ) {
    super(reason);
    this.name = 'PluginStreamBlocked';
    this.blockedChunk = blockedChunk;
  }
}

export type StreamingPluginHandler<P = Record<string, string>> = (
  context: PluginContext,
  parameters: PluginParameters<P>,
  upstream: AsyncIterable<StreamChunk>,
  options?: {
    env: Record<string, any>;
    signal?: AbortSignal;
  }
) => AsyncIterable<StreamChunk>;
