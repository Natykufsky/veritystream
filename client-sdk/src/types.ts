export type MessageType =
  | 'text'
  | 'stream_token'
  | 'stream_end'
  | 'file_share'
  | 'presence'
  | 'typing'
  | 'system';

export interface StreamTokenPayload {
  stream_id: string;
  index: number;
  chunk: string;
  done: boolean;
}

export interface PresencePayload {
  user_id: string;
  status: string;
}

export interface TypingPayload {
  user_id: string;
  is_typing: boolean;
}

export interface FileMetadata {
  id: string;
  room_id: string;
  uploader_id: string;
  file_name: string;
  content_type: string;
  size: number;
  sha256: string;
  created_at: string;
  custom_meta?: Record<string, string>;
}

export interface VerityMessage<T = unknown> {
  id: string;
  room_id: string;
  sender_id: string;
  sender_name?: string;
  type: MessageType;
  payload: T;
  created_at: string;
}

export interface VerityStreamConfig {
  wsUrl?: string;
  sseUrl?: string;
  roomId: string;
  userId: string;
  userName?: string;
  token?: string;
  reconnectIntervalMs?: number;
  maxReconnectAttempts?: number;
}
