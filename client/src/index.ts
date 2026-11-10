import {
  ClientOptions,
  FileMetadata,
  Message,
  MessageListener,
  StatusListener,
  StreamTokenPayload,
  TokenListener
} from './types';

export * from './types';

export class VerityStreamClient {
  private opts: Required<Omit<ClientOptions, 'token' | 'userName'>> & {
    token?: string;
    userName?: string;
  };

  private ws: WebSocket | null = null;
  private sse: EventSource | null = null;
  private messageListeners = new Set<MessageListener>();
  private tokenListeners = new Map<string, Set<TokenListener>>();
  private statusListeners = new Set<StatusListener>();

  // In-memory token stream buffers: streamID -> accumulated string
  private streamBuffers = new Map<string, string>();
  private reconnectAttempts = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private isDestroyed = false;

  constructor(options: ClientOptions) {
    this.opts = {
      url: options.url.replace(/\/$/, ''),
      room: options.room,
      userId: options.userId,
      userName: options.userName || options.userId,
      token: options.token,
      protocol: options.protocol || 'ws',
      fileEndpoint: (options.fileEndpoint || '/api/files').replace(/\/$/, ''),
      autoReconnect: options.autoReconnect ?? true,
      reconnectInterval: options.reconnectInterval ?? 2000,
      maxReconnectAttempts: options.maxReconnectAttempts ?? 10
    };

    this.connect();
  }

  /**
   * Connect or reconnect to the VerityStream server.
   */
  public connect(): void {
    if (this.isDestroyed) return;
    this.cleanup();

    if (this.opts.protocol === 'ws') {
      this.connectWebSocket();
    } else {
      this.connectSSE();
    }
  }

  private connectWebSocket(): void {
    let wsUrl = this.opts.url;
    if (wsUrl.startsWith('http://')) wsUrl = wsUrl.replace('http://', 'ws://');
    if (wsUrl.startsWith('https://')) wsUrl = wsUrl.replace('https://', 'wss://');
    if (!wsUrl.startsWith('ws://') && !wsUrl.startsWith('wss://')) {
      wsUrl = (typeof location !== 'undefined' && location.protocol === 'https:' ? 'wss://' : 'ws://') + wsUrl;
    }

    const query = new URLSearchParams({
      room: this.opts.room,
      user: this.opts.userId,
      name: this.opts.userName || this.opts.userId
    });
    if (this.opts.token) query.set('token', this.opts.token);

    const fullUrl = `${wsUrl}/ws?${query.toString()}`;
    this.notifyStatus(false, 'Connecting (WS)...');

    try {
      this.ws = new WebSocket(fullUrl);

      this.ws.onopen = () => {
        this.reconnectAttempts = 0;
        this.notifyStatus(true, 'Connected (WS)');
      };

      this.ws.onmessage = (event) => {
        try {
          const msg: Message = JSON.parse(event.data);
          this.handleIncoming(msg);
        } catch {
          // Ignore invalid parse
        }
      };

      this.ws.onclose = () => {
        this.notifyStatus(false, 'Disconnected');
        this.scheduleReconnect();
      };

      this.ws.onerror = () => {
        this.notifyStatus(false, 'Connection Error');
      };
    } catch (err) {
      this.notifyStatus(false, 'Failed to connect');
      this.scheduleReconnect();
    }
  }

  private connectSSE(): void {
    let httpUrl = this.opts.url;
    if (httpUrl.startsWith('ws://')) httpUrl = httpUrl.replace('ws://', 'http://');
    if (httpUrl.startsWith('wss://')) httpUrl = httpUrl.replace('wss://', 'https://');

    const query = new URLSearchParams({
      room: this.opts.room,
      user: this.opts.userId
    });
    if (this.opts.token) query.set('token', this.opts.token);

    const fullUrl = `${httpUrl}/sse?${query.toString()}`;
    this.notifyStatus(false, 'Connecting (SSE)...');

    try {
      this.sse = new EventSource(fullUrl);

      this.sse.onopen = () => {
        this.reconnectAttempts = 0;
        this.notifyStatus(true, 'Connected (SSE)');
      };

      const eventTypes = ['text', 'stream_token', 'stream_end', 'file_share', 'presence', 'typing', 'system'];
      eventTypes.forEach((evt) => {
        this.sse?.addEventListener(evt, (e) => {
          try {
            const msg: Message = JSON.parse((e as MessageEvent).data);
            this.handleIncoming(msg);
          } catch {
            // Ignore
          }
        });
      });

      this.sse.onerror = () => {
        this.notifyStatus(false, 'SSE Offline');
        this.scheduleReconnect();
      };
    } catch {
      this.notifyStatus(false, 'Failed to init SSE');
      this.scheduleReconnect();
    }
  }

  private handleIncoming(msg: Message): void {
    // 1. Dispatch to general message listeners
    this.messageListeners.forEach((fn) => fn(msg));

    // 2. Specialized token stream processing
    if (msg.type === 'stream_token' || msg.type === 'stream_end') {
      const payload: StreamTokenPayload =
        typeof msg.payload === 'string' ? JSON.parse(msg.payload) : msg.payload;

      const current = (this.streamBuffers.get(payload.stream_id) || '') + (payload.chunk || '');
      this.streamBuffers.set(payload.stream_id, current);

      const listeners = this.tokenListeners.get(payload.stream_id);
      if (listeners) {
        listeners.forEach((fn) => fn(payload, current));
      }

      // Wildcard stream listener
      const anyStreamListeners = this.tokenListeners.get('*');
      if (anyStreamListeners) {
        anyStreamListeners.forEach((fn) => fn(payload, current));
      }

      if (msg.type === 'stream_end' || payload.done) {
        // Stream completed
      }
    }
  }

  private scheduleReconnect(): void {
    if (this.isDestroyed || !this.opts.autoReconnect) return;
    if (this.reconnectAttempts >= this.opts.maxReconnectAttempts) {
      this.notifyStatus(false, 'Max reconnect attempts reached');
      return;
    }

    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    const delay = this.opts.reconnectInterval * Math.min(Math.pow(1.5, this.reconnectAttempts), 5);
    this.reconnectAttempts++;

    this.reconnectTimer = setTimeout(() => {
      this.connect();
    }, delay);
  }

  private notifyStatus(connected: boolean, status: string): void {
    this.statusListeners.forEach((fn) => fn(connected, status));
  }

  /**
   * Send a standard text chat message.
   */
  public sendText(text: string): boolean {
    return this.publish('text', text);
  }

  /**
   * Broadcast a custom message.
   */
  public publish<T = unknown>(type: Message['type'], payload: T): boolean {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      console.warn('[VerityStream] Cannot publish in SSE or disconnected mode');
      return false;
    }

    const msg: Message<T> = {
      room_id: this.opts.room,
      sender_id: this.opts.userId,
      sender_name: this.opts.userName,
      type,
      payload,
      created_at: new Date().toISOString()
    };

    this.ws.send(JSON.stringify(msg));
    return true;
  }

  /**
   * Upload a file in multi-part chunks with progress tracking.
   */
  public async uploadFile(
    file: File | Blob,
    fileName: string,
    onProgress?: (percent: number, currentChunk: number, totalChunks: number) => void,
    chunkSize = 256 * 1024 // 256 KB
  ): Promise<FileMetadata> {
    let httpUrl = this.opts.url;
    if (httpUrl.startsWith('ws://')) httpUrl = httpUrl.replace('ws://', 'http://');
    if (httpUrl.startsWith('wss://')) httpUrl = httpUrl.replace('wss://', 'https://');

    const totalChunks = Math.ceil(file.size / chunkSize);
    const uploadId = 'up_' + Date.now() + '_' + Math.random().toString(36).substring(2, 8);

    for (let i = 0; i < totalChunks; i++) {
      const start = i * chunkSize;
      const end = Math.min(start + chunkSize, file.size);
      const chunk = file.slice(start, end);

      const chunkUrl = `${httpUrl}${this.opts.fileEndpoint}/chunk?upload_id=${uploadId}&chunk_index=${i}`;
      const res = await fetch(chunkUrl, {
        method: 'POST',
        body: chunk
      });

      if (!res.ok) {
        throw new Error(`Chunk ${i} upload failed with status ${res.status}`);
      }

      const percent = Math.round(((i + 1) / totalChunks) * 100);
      onProgress?.(percent, i + 1, totalChunks);
    }

    // Finalize assembly
    const completeUrl = `${httpUrl}${this.opts.fileEndpoint}/complete`;
    const completeRes = await fetch(completeUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        upload_id: uploadId,
        room_id: this.opts.room,
        uploader_id: this.opts.userId,
        file_name: fileName,
        content_type: file.type || 'application/octet-stream',
        total_chunks: totalChunks
      })
    });

    if (!completeRes.ok) {
      throw new Error(`File assembly failed with status ${completeRes.status}`);
    }

    return completeRes.json();
  }

  /**
   * Subscribe to all room messages.
   */
  public onMessage(fn: MessageListener): () => void {
    this.messageListeners.add(fn);
    return () => this.messageListeners.delete(fn);
  }

  /**
   * Subscribe to incremental AI/log token streams for a given streamId or '*' for all.
   */
  public onTokenStream(streamId: string, fn: TokenListener): () => void {
    if (!this.tokenListeners.has(streamId)) {
      this.tokenListeners.set(streamId, new Set());
    }
    this.tokenListeners.get(streamId)!.add(fn);

    return () => {
      this.tokenListeners.get(streamId)?.delete(fn);
    };
  }

  /**
   * Subscribe to connection status changes.
   */
  public onStatus(fn: StatusListener): () => void {
    this.statusListeners.add(fn);
    return () => this.statusListeners.delete(fn);
  }

  /**
   * Clean up and destroy the client instance.
   */
  public destroy(): void {
    this.isDestroyed = true;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.cleanup();
    this.messageListeners.clear();
    this.tokenListeners.clear();
    this.statusListeners.clear();
  }

  private cleanup(): void {
    if (this.ws) {
      this.ws.onclose = null;
      this.ws.onerror = null;
      this.ws.close();
      this.ws = null;
    }
    if (this.sse) {
      this.sse.close();
      this.sse = null;
    }
  }
}

/**
 * Convenience factory to create a VerityStreamClient.
 */
export function createVerityStream(options: ClientOptions): VerityStreamClient {
  return new VerityStreamClient(options);
}
