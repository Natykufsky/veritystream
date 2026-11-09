import { VerityMessage, VerityStreamConfig } from './types';

export type MessageListener = (msg: VerityMessage) => void;
export type ConnectionStateListener = (state: 'connecting' | 'connected' | 'disconnected' | 'error') => void;

export class VerityStreamClient {
  private config: VerityStreamConfig;
  private ws: WebSocket | null = null;
  private messageListeners: Set<MessageListener> = new Set();
  private stateListeners: Set<ConnectionStateListener> = new Set();
  private reconnectAttempts = 0;
  private isExplicitlyClosed = false;

  constructor(config: VerityStreamConfig) {
    this.config = config;
  }

  public connect(): void {
    if (!this.config.wsUrl) {
      throw new Error('wsUrl is required for WebSocket duplex connection');
    }

    this.isExplicitlyClosed = false;
    this.notifyState('connecting');

    const url = new URL(this.config.wsUrl);
    url.searchParams.set('room', this.config.roomId);
    url.searchParams.set('user', this.config.userId);
    if (this.config.userName) {
      url.searchParams.set('name', this.config.userName);
    }
    if (this.config.token) {
      url.searchParams.set('token', this.config.token);
    }

    try {
      this.ws = new WebSocket(url.toString());

      this.ws.onopen = () => {
        this.reconnectAttempts = 0;
        this.notifyState('connected');
      };

      this.ws.onmessage = (event) => {
        try {
          const msg: VerityMessage = JSON.parse(event.data);
          this.messageListeners.forEach((listener) => listener(msg));
        } catch (err) {
          console.error('[VerityStream] Failed to parse message:', err);
        }
      };

      this.ws.onerror = (err) => {
        console.error('[VerityStream] WebSocket error:', err);
        this.notifyState('error');
      };

      this.ws.onclose = () => {
        this.notifyState('disconnected');
        if (!this.isExplicitlyClosed) {
          this.scheduleReconnect();
        }
      };
    } catch (err) {
      this.notifyState('error');
      this.scheduleReconnect();
    }
  }

  public send(type: VerityMessage['type'], payload: unknown): void {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      throw new Error('[VerityStream] Cannot send message: WebSocket is not open');
    }

    const msg: Partial<VerityMessage> = {
      room_id: this.config.roomId,
      sender_id: this.config.userId,
      sender_name: this.config.userName,
      type,
      payload,
    };

    this.ws.send(JSON.stringify(msg));
  }

  public sendText(text: string): void {
    this.send('text', text);
  }

  public sendTyping(isTyping: boolean): void {
    this.send('typing', { user_id: this.config.userId, is_typing: isTyping });
  }

  public onMessage(listener: MessageListener): () => void {
    this.messageListeners.add(listener);
    return () => this.messageListeners.delete(listener);
  }

  public onStateChange(listener: ConnectionStateListener): () => void {
    this.stateListeners.add(listener);
    return () => this.stateListeners.delete(listener);
  }

  public disconnect(): void {
    this.isExplicitlyClosed = true;
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
    this.notifyState('disconnected');
  }

  private notifyState(state: 'connecting' | 'connected' | 'disconnected' | 'error') {
    this.stateListeners.forEach((listener) => listener(state));
  }

  private scheduleReconnect(): void {
    const maxAttempts = this.config.maxReconnectAttempts ?? 10;
    const interval = this.config.reconnectIntervalMs ?? 3000;

    if (this.reconnectAttempts < maxAttempts) {
      this.reconnectAttempts++;
      const backoff = Math.min(interval * Math.pow(1.5, this.reconnectAttempts - 1), 30000);
      setTimeout(() => {
        if (!this.isExplicitlyClosed) {
          this.connect();
        }
      }, backoff);
    }
  }
}
