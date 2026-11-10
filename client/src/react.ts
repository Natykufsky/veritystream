import { useEffect, useRef, useState, useCallback } from 'react';
import { VerityStreamClient, createVerityStream } from './index';
import type { ClientOptions, Message, StreamTokenPayload, FileMetadata } from './types';

export interface UseVerityStreamReturn {
  client: VerityStreamClient | null;
  isConnected: boolean;
  statusText: string;
  messages: Message[];
  activeTokens: Record<string, string>;
  sendText: (text: string) => boolean;
  publish: <T = unknown>(type: Message['type'], payload: T) => boolean;
  uploadFile: (file: File, onProgress?: (pct: number) => void) => Promise<FileMetadata>;
  clearMessages: () => void;
}

/**
 * React hook to connect, send, and subscribe to VerityStream rooms with token streams.
 */
export function useVerityStream(options: ClientOptions): UseVerityStreamReturn {
  const [isConnected, setIsConnected] = useState(false);
  const [statusText, setStatusText] = useState('Initializing...');
  const [messages, setMessages] = useState<Message[]>([]);
  const [activeTokens, setActiveTokens] = useState<Record<string, string>>({});
  
  const clientRef = useRef<VerityStreamClient | null>(null);

  useEffect(() => {
    const client = createVerityStream(options);
    clientRef.current = client;

    const unStatus = client.onStatus((connected, status) => {
      setIsConnected(connected);
      setStatusText(status);
    });

    const unMsg = client.onMessage((msg) => {
      setMessages((prev) => [...prev, msg]);
    });

    const unToken = client.onTokenStream('*', (token, fullText) => {
      setActiveTokens((prev) => ({
        ...prev,
        [token.stream_id]: fullText
      }));
    });

    return () => {
      unStatus();
      unMsg();
      unToken();
      client.destroy();
    };
  }, [options.url, options.room, options.userId, options.protocol]);

  const sendText = useCallback((text: string) => {
    return clientRef.current?.sendText(text) ?? false;
  }, []);

  const publish = useCallback(<T = unknown>(type: Message['type'], payload: T) => {
    return clientRef.current?.publish(type, payload) ?? false;
  }, []);

  const uploadFile = useCallback((file: File, onProgress?: (pct: number) => void) => {
    if (!clientRef.current) throw new Error('Client not initialized');
    return clientRef.current.uploadFile(file, file.name, (pct) => {
      onProgress?.(pct);
    });
  }, []);

  const clearMessages = useCallback(() => {
    setMessages([]);
    setActiveTokens({});
  }, []);

  return {
    client: clientRef.current,
    isConnected,
    statusText,
    messages,
    activeTokens,
    sendText,
    publish,
    uploadFile,
    clearMessages
  };
}
