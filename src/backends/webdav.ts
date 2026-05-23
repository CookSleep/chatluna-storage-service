import { Readable } from 'stream'
import {
    createStreamingRequestInit,
    readStream,
    StorageBackend,
    StorageResult,
    StorageUploadOptions,
    WebDAVStorageConfig
} from './base'

export class WebDAVStorageBackend implements StorageBackend {
    readonly type = 'webdav' as const
    readonly hasPublicUrl = true

    private basePath: string

    constructor(private config: WebDAVStorageConfig) {
        this.basePath = this.trimSlash(config.basePath ?? 'chatluna-storage')
    }

    async init(): Promise<void> {
        await this.ensureBasePath()
    }

    async upload(buffer: Buffer, filename: string): Promise<StorageResult> {
        const remotePath = `${this.basePath}/temp/${filename}`
        const url = `${this.trimSlash(this.config.endpoint)}/${this.encodePath(remotePath)}`

        const headers: Record<string, string> = {
            'Content-Type': 'application/octet-stream'
        }

        if (this.config.username && this.config.password) {
            headers['Authorization'] =
                `Basic ${this.b64(`${this.config.username}:${this.config.password}`)}`
        }

        // Ensure temp directory exists
        await this.ensureDirectory(`${this.basePath}/temp`)

        const response = await fetch(url, {
            method: 'PUT',
            headers,
            body: new Uint8Array(buffer)
        })

        if (
            !response.ok &&
            response.status !== 201 &&
            response.status !== 204
        ) {
            throw new Error(`WebDAV upload failed: ${response.status}`)
        }

        return {
            key: remotePath,
            publicUrl: url
        }
    }

    async uploadStream(
        stream: NodeJS.ReadableStream,
        filename: string,
        options: StorageUploadOptions = {}
    ): Promise<StorageResult> {
        if (options.size == null) {
            return await this.upload(await readStream(stream), filename)
        }

        const remotePath = `${this.basePath}/temp/${filename}`
        const url = `${this.trimSlash(this.config.endpoint)}/${this.encodePath(remotePath)}`

        const headers: Record<string, string> = {
            'Content-Type': options.mimeType ?? 'application/octet-stream',
            'Content-Length': options.size.toString()
        }

        if (this.config.username && this.config.password) {
            headers['Authorization'] =
                `Basic ${this.b64(`${this.config.username}:${this.config.password}`)}`
        }

        await this.ensureDirectory(`${this.basePath}/temp`)

        const response = await fetch(
            url,
            createStreamingRequestInit(stream, {
                method: 'PUT',
                headers
            })
        )

        if (
            !response.ok &&
            response.status !== 201 &&
            response.status !== 204
        ) {
            throw new Error(`WebDAV upload failed: ${response.status}`)
        }

        return {
            key: remotePath,
            publicUrl: url
        }
    }

    async download(key: string): Promise<Buffer> {
        const url = `${this.trimSlash(this.config.endpoint)}/${this.encodePath(key)}`

        const headers: Record<string, string> = {}

        if (this.config.username && this.config.password) {
            headers['Authorization'] =
                `Basic ${this.b64(`${this.config.username}:${this.config.password}`)}`
        }

        const response = await fetch(url, {
            method: 'GET',
            headers
        })

        if (!response.ok) {
            throw new Error(`WebDAV download failed: ${response.status}`)
        }

        return Buffer.from(await response.arrayBuffer())
    }

    downloadStream(key: string): NodeJS.ReadableStream {
        const self = this
        return Readable.from(
            (async function* () {
                const buf = await self.download(key)
                yield buf
            })()
        )
    }

    async delete(key: string): Promise<void> {
        const url = `${this.trimSlash(this.config.endpoint)}/${this.encodePath(key)}`

        const headers: Record<string, string> = {}

        if (this.config.username && this.config.password) {
            headers['Authorization'] =
                `Basic ${this.b64(`${this.config.username}:${this.config.password}`)}`
        }

        const response = await fetch(url, {
            method: 'DELETE',
            headers
        })

        if (!response.ok && response.status !== 404) {
            throw new Error(`WebDAV delete failed: ${response.status}`)
        }
    }

    async exists(key: string): Promise<boolean> {
        const url = `${this.trimSlash(this.config.endpoint)}/${this.encodePath(key)}`

        const headers: Record<string, string> = {}

        if (this.config.username && this.config.password) {
            headers['Authorization'] =
                `Basic ${this.b64(`${this.config.username}:${this.config.password}`)}`
        }

        const response = await fetch(url, {
            method: 'HEAD',
            headers
        })

        return response.ok
    }

    private async ensureBasePath(): Promise<void> {
        await this.ensureDirectory(this.basePath)
    }

    private async ensureDirectory(path: string): Promise<void> {
        const parts = path.split('/')
        let currentPath = ''

        for (const part of parts) {
            if (!part) continue
            currentPath = currentPath ? `${currentPath}/${part}` : part
            const url = `${this.trimSlash(this.config.endpoint)}/${this.encodePath(currentPath)}/`

            const headers: Record<string, string> = {}

            if (this.config.username && this.config.password) {
                headers['Authorization'] =
                    `Basic ${this.b64(`${this.config.username}:${this.config.password}`)}`
            }

            const response = await fetch(url, {
                method: 'MKCOL',
                headers
            })

            // 405 Method Not Allowed means directory already exists
            // 201 Created means directory was created successfully
            if (
                !response.ok &&
                response.status !== 405 &&
                response.status !== 201
            ) {
                // Ignore errors - directory might already exist
            }
        }
    }

    private b64(str: string): string {
        return Buffer.from(str, 'utf8').toString('base64')
    }

    private trimSlash(s: string): string {
        return s.replace(/\/+$/, '')
    }

    private encodePath(path: string): string {
        return path.split('/').map(encodeURIComponent).join('/')
    }
}
