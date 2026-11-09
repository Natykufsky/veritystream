import { useState, useEffect, useRef, useCallback } from 'react';
import { VerityStreamClient } from './client';
import { VerityMessage, VerityStreamConfig, StreamTokenPayload } from './types';

export interface UseChatStreamReturn {
  messages: VerityMessage[];
  activeStreams: Record<string, string>; // stream_id -> accumulated string
  connectionState: 'connecting' | 'connected' | 'disconnected' | 'error';
  sendMessage: (text: string) => void;
  sendTyping: (isTyping: boolean) => void;
  clearMessages: () => void;
}

export function useChatStream(config: VerityStreamConfig): UseChatStreamReturn {
  const [messages, setMessages] = useState<VerityMessage[]>([]);
  const [activeStreams, setActiveStreams] = useState<Record<string, string>>({});
  const [connectionState, setConnectionState] = useState<'connecting' | 'connected' | 'disconnected' | 'error'>('disconnected');
  
  const clientRef = useRef<VerityStreamClient | null>(null);

  useEffect(() => {
    const client = new VerityStreamClient(config);
    clientRef.current = client;

    const unsubsState = client.onStateChange(setConnectionState);

    const unsubsMsg = client.onMessage((msg) => {
      if (msg.type === 'stream_token') {
        const payload = msg.payload as StreamTokenPayload;
        setActiveStreams((prev) => ({
          ...prev,
          [payload.stream_id]: (prev[payload.stream_id] || '') + payload.chunk,
        }));
      } else if (msg.type === 'stream_end') {
        const payload = msg.payload as StreamTokenPayload;
        setActiveStreams((prev) => {
          const finalContent = (prev[payload.stream_id] || '') + (payload.chunk || '');
          // Promote completed stream to a full message in chat list
          setMessages((msgs) => [
            ...msgs,
            {
              ...msg,
              type: 'text',
              payload: finalContent,
            },
          ]);
          const copy = { ...prev };
          delete copy[payload.stream_id];
          return copy;
        });
      } else {
        setMessages((prev) => [...prev, msg]);
      }
    });

    client.connect();

    return () => {
      unsubsState();
      unsubsMsg();
      client.disconnect();
    };
  }, [config.wsUrl, config.roomId, config.userId]);

  const sendMessage = useCallback((text: string) => {
    clientRef.current?.sendText(text);
  }, []);

  const sendTyping = useCallback((isTyping: boolean) => {
    clientRef.current?.sendTyping(isTyping);
  }, []);

  const clearMessages = useCallback(() => {
    setMessages([]);
    setActiveStreams({});
  }, []);

  return {
    messages,
    activeStreams,
    connectionState,
    sendMessage,
    sendTyping,
    clearMessages,
  };
}
