import { createReadStream, createWriteStream } from 'fs'
import { join } from 'path'
import fs from 'fs/promises'
import { pipeline } from 'stream/promises'
import { StorageBackend, StorageResult, LocalStorageConfig } from './base'

export class LocalStorageBackend implements StorageBackend {
    readonly type = 'local' as const
    readonly hasPublicUrl = false

    private basePath: string

    constructor(private config: LocalStorageConfig) {
        this.basePath = join(config.storagePath, 'temp')
    }

    async init(): Promise<void> {
        await fs.mkdir(this.basePath, { recursive: true })
    }

    async upload(buffer: Buffer, filename: string): Promise<StorageResult> {
        const filePath = join(this.basePath, filename)
        await fs.writeFile(filePath, buffer)

        return {
            key: filePath
        }
    }

    async uploadStream(
        stream: NodeJS.ReadableStream,
        filename: string
    ): Promise<StorageResult> {
        const filePath = join(this.basePath, filename)
        await pipeline(stream, createWriteStream(filePath))

        return {
            key: filePath
        }
    }

    async download(key: string): Promise<Buffer> {
        return fs.readFile(key)
    }

    downloadStream(key: string): NodeJS.ReadableStream {
        return createReadStream(key)
    }

    async delete(key: string): Promise<void> {
        try {
            await fs.unlink(key)
        } catch {
            // Ignore errors if file doesn't exist
        }
    }

    async exists(key: string): Promise<boolean> {
        try {
            await fs.access(key)
            return true
        } catch {
            return false
        }
    }
}
