# @veritystream/client

TypeScript & React client SDK for [VerityStream](https://github.com/Natykufsky/veritystream) — real-time chat, LLM token streaming, and chunked file uploads.

---

## Installation

```bash
npm install @veritystream/client
# or
pnpm add @veritystream/client
# or
yarn add @veritystream/client
```

---

## Vanilla JavaScript / TypeScript Usage

```typescript
import { createVerityStream } from '@veritystream/client';

const client = createVerityStream({
  url: 'https://api.example.com',
  room: 'deploy-42',
  userId: 'user_alice',
  userName: 'Alice',
  protocol: 'ws', // 'ws' for WebSocket (default) or 'sse' for read-only Server-Sent Events
});

// 1. Listen for connection status
client.onStatus((connected, status) => {
  console.log(`[Status] ${status} (connected: ${connected})`);
});

// 2. Listen for regular chat messages
client.onMessage((msg) => {
  console.log(`[Message from ${msg.sender_name}]:`, msg.payload);
});

// 3. Listen for streaming AI / LLM tokens in real time
client.onTokenStream('stream_123', (token, fullAccumulatedText) => {
  console.log(`[Chunk]: ${token.chunk}`);
  if (token.done) {
    console.log('[Stream complete]:', fullAccumulatedText);
  }
});

// 4. Send a text message
client.sendText('Hello team from TypeScript client!');

// 5. Upload large file in resumable chunks
await client.uploadFile(fileBlob, 'build-artifact.zip', (pct, current, total) => {
  console.log(`Uploading: ${pct}% (chunk ${current}/${total})`);
});
```

---

## React Usage (`useVerityStream`)

```tsx
import React, { useState } from 'react';
import { useVerityStream } from '@veritystream/client/react';

export function ChatComponent() {
  const [input, setInput] = useState('');
  
  const {
    isConnected,
    statusText,
    messages,
    activeTokens,
    sendText,
    uploadFile
  } = useVerityStream({
    url: 'https://api.example.com',
    room: 'general',
    userId: 'user_bob',
    userName: 'Bob'
  });

  return (
    <div>
      <div>Status: {statusText} ({isConnected ? 'Online' : 'Offline'})</div>

      {/* Messages Feed */}
      <div style={{ height: 300, overflowY: 'auto' }}>
        {messages.map((m, i) => (
          <div key={i}>
            <strong>{m.sender_name}:</strong> {JSON.stringify(m.payload)}
          </div>
        ))}

        {/* Live LLM Stream Token View */}
        {Object.entries(activeTokens).map(([streamId, text]) => (
          <div key={streamId} style={{ fontFamily: 'monospace', color: '#0ea5e9' }}>
            <span>Streaming ({streamId}):</span> {text}
          </div>
        ))}
      </div>

      {/* Composer */}
      <input
        value={input}
        onChange={(e) => setInput(e.target.value)}
        placeholder="Type a message..."
      />
      <button onClick={() => { sendText(input); setInput(''); }}>Send</button>

      {/* Chunked Upload */}
      <input
        type="file"
        onChange={async (e) => {
          if (e.target.files?.[0]) {
            await uploadFile(e.target.files[0], (pct) => console.log(`${pct}%`));
          }
        }}
      />
    </div>
  );
}
```

---

## License

MIT © [Kufre N Moses](https://github.com/Natykufsky)
