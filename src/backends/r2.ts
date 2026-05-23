import { createHmac, createHash } from 'crypto'
import { Readable } from 'stream'
import {
    createStreamingRequestInit,
    readStream,
    StorageBackend,
    StorageResult,
    R2StorageConfig,
    StorageUploadOptions
} from './base'

/**
 * Cloudflare R2 Storage Backend
 * R2 is S3-compatible, but uses a specific endpoint format
 */
export class R2StorageBackend implements StorageBackend {
    readonly type = 'r2' as const
    readonly hasPublicUrl = true

    private endpoint: string

    constructor(private config: R2StorageConfig) {
        // R2 endpoint format: https://<ACCOUNT_ID>.r2.cloudflarestorage.com
        this.endpoint = `https://${config.accountId}.r2.cloudflarestorage.com`
    }

    async init(): Promise<void> {
        // R2 doesn't require initialization
    }

    async upload(buffer: Buffer, filename: string): Promise<StorageResult> {
        const key = `temp/${filename}`
        const url = `${this.endpoint}/${this.config.bucket}/${key}`

        const date = new Date()
        const amzDate = date.toISOString().replace(/[:-]|\.\d{3}/g, '')
        const dateStamp = amzDate.slice(0, 8)

        const payloadHash = createHash('sha256').update(buffer).digest('hex')

        const headers: Record<string, string> = {
            Host: new URL(url).host,
            'x-amz-date': amzDate,
            'x-amz-content-sha256': payloadHash,
            'Content-Type': 'application/octet-stream',
            'Content-Length': buffer.length.toString()
        }

        const authorization = this.generateAuthorizationHeader(
            'PUT',
            key,
            headers,
            payloadHash,
            dateStamp,
            amzDate
        )

        headers['Authorization'] = authorization

        const response = await fetch(url, {
            method: 'PUT',
            headers,
            body: new Uint8Array(buffer)
        })

        if (!response.ok) {
            const text = await response.text()
            throw new Error(`R2 upload failed: ${response.status} ${text}`)
        }

        return {
            key,
            publicUrl: url
        }
    }

    async uploadStream(
        stream: NodeJS.ReadableStream,
        filename: string,
        options: StorageUploadOptions = {}
    ): Promise<StorageResult> {
        if (options.size == null || !options.hash) {
            return await this.upload(await readStream(stream), filename)
        }

        const key = `temp/${filename}`
        const url = `${this.endpoint}/${this.config.bucket}/${key}`

        const date = new Date()
        const amzDate = date.toISOString().replace(/[:-]|\.\d{3}/g, '')
        const dateStamp = amzDate.slice(0, 8)

        const headers: Record<string, string> = {
            Host: new URL(url).host,
            'x-amz-date': amzDate,
            'x-amz-content-sha256': options.hash,
            'Content-Type': options.mimeType ?? 'application/octet-stream',
            'Content-Length': options.size.toString()
        }

        const authorization = this.generateAuthorizationHeader(
            'PUT',
            key,
            headers,
            options.hash,
            dateStamp,
            amzDate
        )

        headers['Authorization'] = authorization

        const response = await fetch(
            url,
            createStreamingRequestInit(stream, {
                method: 'PUT',
                headers
            })
        )

        if (!response.ok) {
            const text = await response.text()
            throw new Error(`R2 upload failed: ${response.status} ${text}`)
        }

        return {
            key,
            publicUrl: url
        }
    }

    async download(key: string): Promise<Buffer> {
        const url = `${this.endpoint}/${this.config.bucket}/${key}`

        const date = new Date()
        const amzDate = date.toISOString().replace(/[:-]|\.\d{3}/g, '')
        const dateStamp = amzDate.slice(0, 8)

        const payloadHash = 'UNSIGNED-PAYLOAD'

        const headers: Record<string, string> = {
            Host: new URL(url).host,
            'x-amz-date': amzDate,
            'x-amz-content-sha256': payloadHash
        }

        const authorization = this.generateAuthorizationHeader(
            'GET',
            key,
            headers,
            payloadHash,
            dateStamp,
            amzDate
        )

        headers['Authorization'] = authorization

        const response = await fetch(url, {
            method: 'GET',
            headers
        })

        if (!response.ok) {
            throw new Error(`R2 download failed: ${response.status}`)
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
        const url = `${this.endpoint}/${this.config.bucket}/${key}`

        const date = new Date()
        const amzDate = date.toISOString().replace(/[:-]|\.\d{3}/g, '')
        const dateStamp = amzDate.slice(0, 8)

        const payloadHash = createHash('sha256').update('').digest('hex')

        const headers: Record<string, string> = {
            Host: new URL(url).host,
            'x-amz-date': amzDate,
            'x-amz-content-sha256': payloadHash
        }

        const authorization = this.generateAuthorizationHeader(
            'DELETE',
            key,
            headers,
            payloadHash,
            dateStamp,
            amzDate
        )

        headers['Authorization'] = authorization

        const response = await fetch(url, {
            method: 'DELETE',
            headers
        })

        if (!response.ok && response.status !== 404) {
            throw new Error(`R2 delete failed: ${response.status}`)
        }
    }

    async exists(key: string): Promise<boolean> {
        const url = `${this.endpoint}/${this.config.bucket}/${key}`

        const date = new Date()
        const amzDate = date.toISOString().replace(/[:-]|\.\d{3}/g, '')
        const dateStamp = amzDate.slice(0, 8)

        const payloadHash = 'UNSIGNED-PAYLOAD'

        const headers: Record<string, string> = {
            Host: new URL(url).host,
            'x-amz-date': amzDate,
            'x-amz-content-sha256': payloadHash
        }

        const authorization = this.generateAuthorizationHeader(
            'HEAD',
            key,
            headers,
            payloadHash,
            dateStamp,
            amzDate
        )

        headers['Authorization'] = authorization

        const response = await fetch(url, {
            method: 'HEAD',
            headers
        })

        return response.ok
    }

    private generateAuthorizationHeader(
        method: string,
        key: string,
        headers: Record<string, string>,
        payloadHash: string,
        dateStamp: string,
        amzDate: string
    ): string {
        // R2 uses 'auto' as region
        const region = 'auto'
        const service = 's3'

        const canonicalUri = `/${this.config.bucket}/${key}`
        const canonicalQueryString = ''

        const signedHeaders = Object.keys(headers)
            .map((h) => h.toLowerCase())
            .sort()
            .join(';')

        const canonicalHeaders =
            Object.keys(headers)
                .map((h) => h.toLowerCase())
                .sort()
                .map(
                    (h) =>
                        `${h}:${headers[Object.keys(headers).find((k) => k.toLowerCase() === h)!].trim()}`
                )
                .join('\n') + '\n'

        const canonicalRequest = [
            method,
            canonicalUri,
            canonicalQueryString,
            canonicalHeaders,
            signedHeaders,
            payloadHash
        ].join('\n')

        const algorithm = 'AWS4-HMAC-SHA256'
        const credentialScope = `${dateStamp}/${region}/${service}/aws4_request`
        const stringToSign = [
            algorithm,
            amzDate,
            credentialScope,
            createHash('sha256').update(canonicalRequest).digest('hex')
        ].join('\n')

        const signingKey = this.getSigningKey(dateStamp, region, service)
        const signature = createHmac('sha256', signingKey)
            .update(stringToSign)
            .digest('hex')

        return `${algorithm} Credential=${this.config.accessKeyId}/${credentialScope}, SignedHeaders=${signedHeaders}, Signature=${signature}`
    }

    private getSigningKey(
        dateStamp: string,
        region: string,
        service: string
    ): Buffer {
        const kDate = createHmac('sha256', `AWS4${this.config.secretAccessKey}`)
            .update(dateStamp)
            .digest()
        const kRegion = createHmac('sha256', kDate).update(region).digest()
        const kService = createHmac('sha256', kRegion).update(service).digest()
        const kSigning = createHmac('sha256', kService)
            .update('aws4_request')
            .digest()
        return kSigning
    }
}
