import { createHmac, createHash } from 'crypto'
import { Readable } from 'stream'
import {
    createStreamingRequestInit,
    readStream,
    StorageBackend,
    StorageResult,
    S3StorageConfig,
    StorageUploadOptions
} from './base'

export class S3StorageBackend implements StorageBackend {
    readonly type = 's3' as const
    readonly hasPublicUrl = true

    constructor(private config: S3StorageConfig) {}

    async init(): Promise<void> {
        // S3 doesn't require initialization
    }

    async upload(buffer: Buffer, filename: string): Promise<StorageResult> {
        const key = `temp/${filename}`
        const request = this.buildRequestTarget(key)

        const date = new Date()
        const amzDate = date.toISOString().replace(/[:-]|\.\d{3}/g, '')
        const dateStamp = amzDate.slice(0, 8)

        const payloadHash = createHash('sha256').update(buffer).digest('hex')

        const headers: Record<string, string> = {
            Host: request.host,
            'x-amz-date': amzDate,
            'x-amz-content-sha256': payloadHash,
            'Content-Type': 'application/octet-stream',
            'Content-Length': buffer.length.toString()
        }

        const authorization = this.generateAuthorizationHeader(
            'PUT',
            request.canonicalUri,
            headers,
            payloadHash,
            dateStamp,
            amzDate
        )

        headers['Authorization'] = authorization

        const response = await fetch(request.url, {
            method: 'PUT',
            headers,
            body: new Uint8Array(buffer)
        })

        if (!response.ok) {
            const text = await response.text()
            throw new Error(`S3 upload failed: ${response.status} ${text}`)
        }

        return {
            key,
            publicUrl: request.url
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
        const request = this.buildRequestTarget(key)

        const date = new Date()
        const amzDate = date.toISOString().replace(/[:-]|\.\d{3}/g, '')
        const dateStamp = amzDate.slice(0, 8)

        const headers: Record<string, string> = {
            Host: request.host,
            'x-amz-date': amzDate,
            'x-amz-content-sha256': options.hash,
            'Content-Type': options.mimeType ?? 'application/octet-stream',
            'Content-Length': options.size.toString()
        }

        const authorization = this.generateAuthorizationHeader(
            'PUT',
            request.canonicalUri,
            headers,
            options.hash,
            dateStamp,
            amzDate
        )

        headers['Authorization'] = authorization

        const response = await fetch(
            request.url,
            createStreamingRequestInit(stream, {
                method: 'PUT',
                headers
            })
        )

        if (!response.ok) {
            const text = await response.text()
            throw new Error(`S3 upload failed: ${response.status} ${text}`)
        }

        return {
            key,
            publicUrl: request.url
        }
    }

    async download(key: string): Promise<Buffer> {
        const request = this.buildRequestTarget(key)

        const date = new Date()
        const amzDate = date.toISOString().replace(/[:-]|\.\d{3}/g, '')
        const dateStamp = amzDate.slice(0, 8)

        const payloadHash = 'UNSIGNED-PAYLOAD'

        const headers: Record<string, string> = {
            Host: request.host,
            'x-amz-date': amzDate,
            'x-amz-content-sha256': payloadHash
        }

        const authorization = this.generateAuthorizationHeader(
            'GET',
            request.canonicalUri,
            headers,
            payloadHash,
            dateStamp,
            amzDate
        )

        headers['Authorization'] = authorization

        const response = await fetch(request.url, {
            method: 'GET',
            headers
        })

        if (!response.ok) {
            throw new Error(`S3 download failed: ${response.status}`)
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
        const request = this.buildRequestTarget(key)

        const date = new Date()
        const amzDate = date.toISOString().replace(/[:-]|\.\d{3}/g, '')
        const dateStamp = amzDate.slice(0, 8)

        const payloadHash = createHash('sha256').update('').digest('hex')

        const headers: Record<string, string> = {
            Host: request.host,
            'x-amz-date': amzDate,
            'x-amz-content-sha256': payloadHash
        }

        const authorization = this.generateAuthorizationHeader(
            'DELETE',
            request.canonicalUri,
            headers,
            payloadHash,
            dateStamp,
            amzDate
        )

        headers['Authorization'] = authorization

        const response = await fetch(request.url, {
            method: 'DELETE',
            headers
        })

        if (!response.ok && response.status !== 404) {
            throw new Error(`S3 delete failed: ${response.status}`)
        }
    }

    async exists(key: string): Promise<boolean> {
        const request = this.buildRequestTarget(key)

        const date = new Date()
        const amzDate = date.toISOString().replace(/[:-]|\.\d{3}/g, '')
        const dateStamp = amzDate.slice(0, 8)

        const payloadHash = 'UNSIGNED-PAYLOAD'

        const headers: Record<string, string> = {
            Host: request.host,
            'x-amz-date': amzDate,
            'x-amz-content-sha256': payloadHash
        }

        const authorization = this.generateAuthorizationHeader(
            'HEAD',
            request.canonicalUri,
            headers,
            payloadHash,
            dateStamp,
            amzDate
        )

        headers['Authorization'] = authorization

        const response = await fetch(request.url, {
            method: 'HEAD',
            headers
        })

        return response.ok
    }

    private buildRequestTarget(key: string): S3RequestTarget {
        const endpoint = new URL(this.config.endpoint)
        const shouldUsePathStyle =
            (this.config.pathStyle ?? true) ||
            endpoint.hostname === 'localhost' ||
            endpoint.hostname.match(/^(\d{1,3}\.){3}\d{1,3}$/)
        const encodedKey = this.encodePath(key)
        const basePath = endpoint.pathname.replace(/\/+$/, '')

        if (shouldUsePathStyle) {
            const path = this.joinUrlPath(
                basePath,
                this.config.bucket,
                encodedKey
            )

            return {
                url: `${endpoint.origin}${path}`,
                host: endpoint.host,
                canonicalUri: path
            }
        }

        const host = `${this.config.bucket}.${endpoint.host}`
        const path = this.joinUrlPath(basePath, encodedKey)

        return {
            url: `${endpoint.protocol}//${host}${path}`,
            host,
            canonicalUri: path
        }
    }

    private generateAuthorizationHeader(
        method: string,
        canonicalUri: string,
        headers: Record<string, string>,
        payloadHash: string,
        dateStamp: string,
        amzDate: string
    ): string {
        const region = this.config.region
        const service = 's3'

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

    private joinUrlPath(...segments: string[]): string {
        return (
            '/' +
            segments
                .map((segment) => segment.replace(/^\/+|\/+$/g, ''))
                .filter(Boolean)
                .join('/')
        )
    }

    private encodePath(path: string): string {
        return path
            .split('/')
            .map((segment) => encodeURIComponent(segment))
            .join('/')
    }
}

interface S3RequestTarget {
    url: string
    host: string
    canonicalUri: string
}
