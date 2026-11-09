import { FileMetadata } from './types';

export interface ChunkUploadOptions {
  uploadBaseUrl: string;
  roomId: string;
  uploaderId: string;
  file: File;
  chunkSize?: number; // default: 1MB
  onProgress?: (percent: number, uploadedBytes: number, totalBytes: number) => void;
  token?: string;
}

export class VerityFileUploader {
  private static async computeSHA256(file: File): Promise<string> {
    const buffer = await file.arrayBuffer();
    const hashBuffer = await crypto.subtle.digest('SHA-256', buffer);
    const hashArray = Array.from(new Uint8Array(hashBuffer));
    return hashArray.map((b) => b.toString(16).padStart(2, '0')).join('');
  }

  public static async uploadChunked(opts: ChunkUploadOptions): Promise<FileMetadata> {
    const chunkSize = opts.chunkSize ?? 1024 * 1024; // 1MB
    const totalBytes = opts.file.size;
    const totalChunks = Math.ceil(totalBytes / chunkSize);
    const uploadId = crypto.randomUUID();

    const headers: Record<string, string> = {};
    if (opts.token) {
      headers['Authorization'] = `Bearer ${opts.token}`;
    }

    let uploadedBytes = 0;

    // 1. Upload chunks sequentially
    for (let index = 0; index < totalChunks; index++) {
      const start = index * chunkSize;
      const end = Math.min(start + chunkSize, totalBytes);
      const chunk = opts.file.slice(start, end);

      const chunkUrl = new URL(`${opts.uploadBaseUrl}/chunk`);
      chunkUrl.searchParams.set('upload_id', uploadId);
      chunkUrl.searchParams.set('chunk_index', index.toString());

      const res = await fetch(chunkUrl.toString(), {
        method: 'POST',
        headers,
        body: chunk,
      });

      if (!res.ok) {
        throw new Error(`Failed to upload chunk ${index}: ${res.statusText}`);
      }

      uploadedBytes += (end - start);
      if (opts.onProgress) {
        const percent = Math.min(100, Math.round((uploadedBytes / totalBytes) * 100));
        opts.onProgress(percent, uploadedBytes, totalBytes);
      }
    }

    // 2. Compute checksum for verification
    const sha256 = await this.computeSHA256(opts.file);

    // 3. Assemble chunks
    const completeUrl = `${opts.uploadBaseUrl}/complete`;
    const assembleRes = await fetch(completeUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...headers,
      },
      body: JSON.stringify({
        upload_id: uploadId,
        room_id: opts.roomId,
        uploader_id: opts.uploaderId,
        file_name: opts.file.name,
        content_type: opts.file.type || 'application/octet-stream',
        total_chunks: totalChunks,
        expected_sha256: sha256,
      }),
    });

    if (!assembleRes.ok) {
      throw new Error(`Failed to assemble file chunks: ${assembleRes.statusText}`);
    }

    return (await assembleRes.json()) as FileMetadata;
  }
}
