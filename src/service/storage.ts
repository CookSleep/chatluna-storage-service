import { createHash } from 'crypto'
import { createReadStream, createWriteStream } from 'fs'
import os from 'os'
import path from 'path'
import { Transform } from 'stream'
import { pipeline } from 'stream/promises'
import { Context, Service, Time } from 'koishi'
import { Config, logger } from '..'
import { TempFileInfo, TempFileInfoWithData } from '../types'
import {
    computeHash,
    getImageType,
    getMimeTypeFromFilename,
    randomFileName
} from '../utils'
import {
    StorageBackend,
    createStorageBackend,
    StorageConfig
} from '../backends'
import fs from 'fs/promises'

interface LRUNode {
    fileId: string
    prev: LRUNode | null
    next: LRUNode | null
}
export class ChatLunaStorageService extends Service {
    private lruHead: LRUNode
    private lruTail: LRUNode
    private lruMap: Map<string, LRUNode>

    private backendPath: string
    private storageBackend: StorageBackend

    constructor(
        ctx: Context,
        public config: Config
    ) {
        super(ctx, 'chatluna_storage', true)

        this.lruHead = { fileId: '', prev: null, next: null }
        this.lruTail = { fileId: '', prev: null, next: null }
        this.lruHead.next = this.lruTail
        this.lruTail.prev = this.lruHead
        this.lruMap = new Map()

        this.backendPath = this.config.backendPath

        // Initialize storage backend based on config
        this.storageBackend = createStorageBackend(this.getStorageConfig())

        ctx.database.extend(
            'chatluna_storage_temp',
            {
                id: { type: 'string', length: 254 },
                path: 'string',
                name: 'string',
                type: {
                    type: 'string',
                    nullable: true
                },
                expireTime: 'timestamp',
                size: 'integer',
                accessTime: 'timestamp',
                accessCount: 'integer',
                // New optional fields for multi-backend support
                storageType: {
                    type: 'string',
                    nullable: true
                },
                publicUrl: {
                    type: 'string',
                    nullable: true
                },
                hash: {
                    type: 'string',
                    initial: '',
                    nullable: false
                }
            },
            {
                autoInc: false,
                primary: 'id'
            }
        )

        this.setupAutoDelete()
        this.initializeLRU()
        this.initStorageBackend()

        ctx.inject(['server'], (ctx) => {
            const backendPath = `${config.serverPath ?? ctx.server.selfUrl}${this.config.backendPath}`
            this.backendPath = backendPath
        })
    }

    private getStorageConfig(): StorageConfig {
        const backendType = this.config.storageBackend ?? 'local'

        switch (backendType) {
            case 's3':
                return {
                    type: 's3',
                    endpoint: this.config.s3Endpoint!,
                    bucket: this.config.s3Bucket!,
                    region: this.config.s3Region!,
                    accessKeyId: this.config.s3AccessKeyId!,
                    secretAccessKey: this.config.s3SecretAccessKey!,
                    pathStyle: this.config.s3PathStyle
                }
            case 'webdav':
                return {
                    type: 'webdav',
                    endpoint: this.config.webdavEndpoint!,
                    username: this.config.webdavUsername,
                    password: this.config.webdavPassword,
                    basePath: this.config.webdavBasePath
                }
            case 'r2':
                return {
                    type: 'r2',
                    accountId: this.config.r2AccountId!,
                    bucket: this.config.r2Bucket!,
                    accessKeyId: this.config.r2AccessKeyId!,
                    secretAccessKey: this.config.r2SecretAccessKey!
                }
            case 'local':
            default:
                return {
                    type: 'local',
                    storagePath: this.config.storagePath
                }
        }
    }

    private async initStorageBackend() {
        try {
            await this.storageBackend.init()
            logger.info(
                `Storage backend initialized: ${this.storageBackend.type}`
            )
        } catch (error) {
            logger.error('Failed to initialize storage backend:', error)
        }
    }

    private async initializeLRU() {
        const files = await this.ctx.database.get('chatluna_storage_temp', {})
        files.sort((a, b) => b.accessTime.getTime() - a.accessTime.getTime())
        for (const file of files) {
            this.addToLRU(file.id)
        }
    }

    private addToLRU(fileId: string) {
        if (this.lruMap.has(fileId)) {
            this.removeFromLRU(fileId)
        }

        const newNode: LRUNode = {
            fileId,
            prev: this.lruHead,
            next: this.lruHead.next
        }
        this.lruHead.next!.prev = newNode
        this.lruHead.next = newNode
        this.lruMap.set(fileId, newNode)
    }

    private removeFromLRU(fileId: string) {
        const node = this.lruMap.get(fileId)
        if (node) {
            node.prev!.next = node.next
            node.next!.prev = node.prev
            this.lruMap.delete(fileId)
        }
    }

    private getLRUVictim(): string | null {
        if (this.lruTail.prev === this.lruHead) return null
        const victim = this.lruTail.prev!
        return victim.fileId
    }

    private async cleanupByStorageSize() {
        const files = await this.ctx.database.get('chatluna_storage_temp', {})
        const totalSize = files.reduce((sum, file) => sum + file.size, 0)
        const maxSizeBytes = this.config.maxStorageSize * 1024 * 1024

        if (totalSize <= maxSizeBytes) return

        const sortedFiles = files.sort(
            (a, b) => a.accessTime.getTime() - b.accessTime.getTime()
        )
        let currentSize = totalSize

        for (const file of sortedFiles) {
            if (currentSize <= maxSizeBytes * 0.8) break
            currentSize -= file.size
            await this.removeFile(file)
        }
    }

    private async cleanupByFileCount() {
        const files = await this.ctx.database.get('chatluna_storage_temp', {})

        if (files.length <= this.config.maxStorageCount) return

        const sortedFiles = files.sort(
            (a, b) => a.accessTime.getTime() - b.accessTime.getTime()
        )
        const filesToDelete =
            files.length - Math.floor(this.config.maxStorageCount * 0.8)

        for (let i = 0; i < filesToDelete; i++) {
            await this.removeFile(sortedFiles[i])
        }
    }

    private downloadFile(file: TempFileInfo): Promise<Buffer> {
        const storageType = file.storageType ?? 'local'
        if (
            storageType !== 'local' &&
            this.storageBackend.type === storageType
        ) {
            return this.storageBackend.download(file.path)
        }
        return fs.readFile(file.path)
    }

    private async removeFile(file: TempFileInfo): Promise<void> {
        try {
            await this.deleteFileFromBackend(file)
        } catch {
            // best-effort delete; always clean up DB record
        }
        await this.ctx.database.remove('chatluna_storage_temp', { id: file.id })
        this.removeFromLRU(file.id)
    }

    private async deleteFileFromBackend(file: TempFileInfo): Promise<void> {
        const storageType = file.storageType ?? 'local'
        if (storageType === 'local') {
            await fs.unlink(file.path)
        } else {
            // For mismatched backend types (migration), errors are silently ignored
            try {
                await this.storageBackend.delete(file.path)
            } catch (e) {
                if (this.storageBackend.type === storageType) throw e
            }
        }
    }

    private setupAutoDelete() {
        const ctx = this.ctx

        const execute = async () => {
            if (!ctx.scope.isActive) return

            const expiredFiles = await ctx.database.get(
                'chatluna_storage_temp',
                {
                    expireTime: { $lt: new Date(Date.now()) }
                }
            )

            if (expiredFiles.length === 0) return

            for (const file of expiredFiles) {
                await this.removeFile(file)
            }

            logger.success(
                `Auto deleted ${expiredFiles.length} expired temp files`
            )
        }

        const executeCleanup = async () => {
            if (!ctx.scope.isActive) return
            await this.cleanupByStorageSize()
            await this.cleanupByFileCount()
        }

        execute()
        executeCleanup()

        ctx.setInterval(async () => {
            await execute()
            await executeCleanup()
        }, Time.minute * 5)
    }

    async createTempFile(
        buffer: Buffer,
        filename: string,
        expireHours?: number,
        mimeType?: string
    ): Promise<TempFileInfoWithData<Buffer>> {
        // Compute hash first to detect duplicates
        const hash = computeHash(buffer)

        // Check for an existing file with the same content hash
        const existing = await this.ctx.database.get('chatluna_storage_temp', {
            hash
        })

        if (existing.length > 0) {
            const dup = existing[0]
            const currentTime = new Date()

            // Refresh access metadata on the deduplicated file
            await this.ctx.database.set(
                'chatluna_storage_temp',
                { id: dup.id },
                {
                    accessTime: currentTime,
                    accessCount: dup.accessCount + 1
                }
            )
            this.addToLRU(dup.id)

            const url = dup.publicUrl ?? `${this.backendPath}/temp/${dup.name}`
            return {
                ...dup,
                accessTime: currentTime,
                accessCount: dup.accessCount + 1,
                data: this.downloadFile(dup),
                url
            }
        }

        let randomName = randomFileName(filename)
        const imageType = getImageType(buffer, true, true)
        if (imageType != null) {
            randomName =
                (randomName.split('.')?.[0] ?? randomName) + '.' + imageType
        }
        const fileType =
            mimeType ??
            getImageType(buffer) ??
            getMimeTypeFromFilename(randomName)

        // Upload to storage backend
        const result = await this.storageBackend.upload(buffer, randomName)

        const expireTime = new Date(
            Date.now() +
                (expireHours || this.config.tempCacheTime) * 60 * 60 * 1000
        )
        const currentTime = new Date()
        const fileInfo: TempFileInfo = {
            id: randomName.split('.')[0],
            path: result.key,
            name: randomName,
            type: fileType,
            expireTime,
            size: buffer.length,
            accessTime: currentTime,
            accessCount: 1,
            storageType: this.storageBackend.type,
            publicUrl: result.publicUrl,
            hash
        }

        await this.ctx.database.create('chatluna_storage_temp', fileInfo)
        this.addToLRU(fileInfo.id)

        const url = result.publicUrl ?? `${this.backendPath}/temp/${randomName}`
        return {
            ...fileInfo,
            data: Promise.resolve(buffer),
            url
        }
    }

    async createTempFileFromStream(
        stream: NodeJS.ReadableStream,
        filename: string,
        meta: {
            expireHours?: number
            mimeType?: string
            size?: number
        } = {}
    ): Promise<TempFileInfoWithData<Buffer>> {
        const dir = await fs.mkdtemp(
            path.join(os.tmpdir(), 'chatluna-storage-')
        )
        const tempPath = path.join(dir, randomFileName(filename))
        const hash = createHash('sha256')
        let size = 0

        const meter = new Transform({
            transform(chunk, _encoding, callback) {
                const data = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)
                size += data.length
                hash.update(data)
                callback(null, data)
            }
        })

        try {
            await pipeline(stream, meter, createWriteStream(tempPath))

            const digest = hash.digest('hex')
            const existing = await this.ctx.database.get(
                'chatluna_storage_temp',
                {
                    hash: digest
                }
            )

            if (existing.length > 0) {
                const dup = existing[0]
                const currentTime = new Date()

                await this.ctx.database.set(
                    'chatluna_storage_temp',
                    { id: dup.id },
                    {
                        accessTime: currentTime,
                        accessCount: dup.accessCount + 1
                    }
                )
                this.addToLRU(dup.id)

                const url =
                    dup.publicUrl ?? `${this.backendPath}/temp/${dup.name}`
                return {
                    ...dup,
                    accessTime: currentTime,
                    accessCount: dup.accessCount + 1,
                    data: this.downloadFile(dup),
                    url
                }
            }

            const randomName = randomFileName(filename)
            const result = await this.storageBackend.uploadStream(
                createReadStream(tempPath),
                randomName,
                {
                    size,
                    hash: digest,
                    mimeType: meta.mimeType
                }
            )

            const expireTime = new Date(
                Date.now() +
                    (meta.expireHours || this.config.tempCacheTime) *
                        60 *
                        60 *
                        1000
            )
            const currentTime = new Date()
            const fileInfo: TempFileInfo = {
                id: randomName.split('.')[0],
                path: result.key,
                name: randomName,
                type: meta.mimeType,
                expireTime,
                size,
                accessTime: currentTime,
                accessCount: 1,
                storageType: this.storageBackend.type,
                publicUrl: result.publicUrl,
                hash: digest
            }

            await this.ctx.database.create('chatluna_storage_temp', fileInfo)
            this.addToLRU(fileInfo.id)

            const url =
                result.publicUrl ?? `${this.backendPath}/temp/${randomName}`
            return {
                ...fileInfo,
                data: this.downloadFile(fileInfo),
                url
            }
        } finally {
            await fs.unlink(tempPath).catch(() => undefined)
            await fs.rmdir(dir).catch(() => undefined)
        }
    }

    async getTempFile(
        id: string
    ): Promise<TempFileInfoWithData<Buffer> | null> {
        let fileInfo = await this.ctx.database.get('chatluna_storage_temp', {
            id
        })

        if (fileInfo.length === 0) {
            fileInfo = await this.ctx.database.get('chatluna_storage_temp', {
                name: id
            })
        }

        if (fileInfo.length === 0) return null

        const file = fileInfo[0]

        const currentTime = new Date()
        await this.ctx.database.set(
            'chatluna_storage_temp',
            { id: file.id },
            {
                accessTime: currentTime,
                accessCount: file.accessCount + 1
            }
        )

        this.addToLRU(id)

        try {
            const dataPromise = this.downloadFile(file)

            // Backfill missing hash: compute and persist asynchronously
            if (!file.hash) {
                dataPromise
                    .then((data) => {
                        const hash = computeHash(data)
                        this.ctx.database
                            .set(
                                'chatluna_storage_temp',
                                { id: file.id },
                                { hash }
                            )
                            .catch((err) =>
                                logger.warn(
                                    'Failed to backfill hash for file',
                                    file.id,
                                    err
                                )
                            )
                    })
                    .catch(() => {})
            }

            const url =
                file.publicUrl ?? `${this.backendPath}/temp/${file.name}`
            return {
                ...file,
                accessTime: currentTime,
                accessCount: file.accessCount + 1,
                data: dataPromise,
                url
            }
        } catch (error) {
            await this.ctx.database.remove('chatluna_storage_temp', { id })
            this.removeFromLRU(id)
            return null
        }
    }

    async deleteTempFile(id: string): Promise<boolean> {
        let fileInfo = await this.ctx.database.get('chatluna_storage_temp', {
            id
        })

        if (fileInfo.length === 0) {
            fileInfo = await this.ctx.database.get('chatluna_storage_temp', {
                name: id
            })
        }

        if (fileInfo.length === 0) return false

        await this.removeFile(fileInfo[0])
        return true
    }
}

declare module 'koishi' {
    interface Context {
        chatluna_storage: ChatLunaStorageService
    }

    interface Tables {
        chatluna_storage_temp: TempFileInfo
    }
}
