export type MessageType =
  | 'text'
  | 'stream_token'
  | 'stream_end'
  | 'file_share'
  | 'presence'
  | 'typing'
  | 'system';

export interface Message<T = unknown> {
  id?: string;
  room_id: string;
  sender_id: string;
  sender_name?: string;
  type: MessageType;
  payload: T;
  created_at: string;
}

export interface StreamTokenPayload {
  stream_id: string;
  index: number;
  chunk: string;
  done: boolean;
}

export interface FileMetadata {
  id: string;
  room_id: string;
  uploader_id: string;
  file_name: string;
  content_type: string;
  size: number;
  sha256?: string;
  created_at: string;
  custom_meta?: Record<string, string>;
}

export interface PresencePayload {
  user_id: string;
  status: string;
}

export interface TypingPayload {
  user_id: string;
  is_typing: boolean;
}

export interface ClientOptions {
  /** WebSocket or HTTP base URL (e.g. "wss://api.example.com" or "https://api.example.com") */
  url: string;
  room: string;
  userId: string;
  userName?: string;
  token?: string;
  /** 'ws' for bidirectional WebSocket, 'sse' for read-only Server-Sent Events (defaults to 'ws') */
  protocol?: 'ws' | 'sse';
  /** Base path for file uploads/downloads (defaults to "/api/files") */
  fileEndpoint?: string;
  /** Automatically reconnect on network drop (defaults to true) */
  autoReconnect?: boolean;
  reconnectInterval?: number;
  maxReconnectAttempts?: number;
}

export type MessageListener<T = unknown> = (msg: Message<T>) => void;
export type TokenListener = (token: StreamTokenPayload, fullText: string) => void;
export type StatusListener = (connected: boolean, status: string) => void;
