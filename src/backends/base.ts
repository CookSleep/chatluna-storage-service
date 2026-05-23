import { Readable } from 'stream'

/**
 * Storage backend interface and types
 */

export interface StorageResult {
    /** Unique key/path for the stored file */
    key: string
    /** Public URL if backend supports it, undefined otherwise */
    publicUrl?: string
}

export interface StorageUploadOptions {
    size?: number
    hash?: string
    mimeType?: string
}

export interface StorageBackend {
    /** Backend type identifier */
    readonly type: StorageBackendType

    /** Whether this backend provides public URLs */
    readonly hasPublicUrl: boolean

    /**
     * Upload a file to storage
     * @param buffer File content
     * @param filename Original filename
     * @returns Storage result with key and optional public URL
     */
    upload(buffer: Buffer, filename: string): Promise<StorageResult>

    uploadStream(
        stream: NodeJS.ReadableStream,
        filename: string,
        options?: StorageUploadOptions
    ): Promise<StorageResult>

    /**
     * Download a file from storage
     * @param key File key/path
     * @returns File content as Buffer
     */
    download(key: string): Promise<Buffer>

    /**
     * Download a file from storage as a readable stream.
     * @param key File key/path
     * @returns Readable stream of file content
     */
    downloadStream(key: string): NodeJS.ReadableStream

    /**
     * Delete a file from storage
     * @param key File key/path
     */
    delete(key: string): Promise<void>

    /**
     * Check if a file exists in storage
     * @param key File key/path
     */
    exists(key: string): Promise<boolean>

    /**
     * Initialize the storage backend
     */
    init(): Promise<void>
}

export type StorageBackendType = 'local' | 's3' | 'webdav' | 'r2'

export interface LocalStorageConfig {
    type: 'local'
    storagePath: string
}

export interface S3StorageConfig {
    type: 's3'
    endpoint: string
    bucket: string
    region: string
    accessKeyId: string
    secretAccessKey: string
    pathStyle?: boolean
}

export interface WebDAVStorageConfig {
    type: 'webdav'
    endpoint: string
    username?: string
    password?: string
    basePath?: string
}

export interface R2StorageConfig {
    type: 'r2'
    accountId: string
    bucket: string
    accessKeyId: string
    secretAccessKey: string
}

export type StorageConfig =
    | LocalStorageConfig
    | S3StorageConfig
    | WebDAVStorageConfig
    | R2StorageConfig

export async function readStream(stream: NodeJS.ReadableStream) {
    const chunks: Buffer[] = []
    for await (const chunk of stream) {
        chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk))
    }
    return Buffer.concat(chunks)
}

async function* toUint8ArrayStream(
    stream: NodeJS.ReadableStream
): AsyncIterable<Uint8Array> {
    for await (const chunk of stream) {
        yield typeof chunk === 'string' ? Buffer.from(chunk) : chunk
    }
}

type StreamingRequestInit = Omit<RequestInit, 'body'> & {
    duplex?: 'half'
}

type FetchRequestInit = RequestInit & {
    duplex?: 'half'
}

type StreamingRequestBody = NonNullable<RequestInit['body']>

export function createStreamingRequestInit(
    stream: NodeJS.ReadableStream,
    init: StreamingRequestInit
): FetchRequestInit {
    const body = Readable.toWeb(
        Readable.from(toUint8ArrayStream(stream))
    ) as unknown as StreamingRequestBody

    return {
        ...init,
        body,
        duplex: init.duplex ?? 'half'
    }
}
