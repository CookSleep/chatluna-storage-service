import { createHash } from 'crypto'
import FileType from 'file-type'

/**
 * Computes the SHA-256 hash of the given buffer and returns it as a hex string.
 * SHA-256 is cryptographically modern and collision-resistant.
 */
export function computeHash(buffer: Buffer): string {
    return createHash('sha256').update(buffer).digest('hex')
}

/**
 * Detect MIME type and extension from a buffer using magic bytes.
 * Returns `{ mime, ext }` or `undefined` if unrecognized.
 */
export async function detectFileType(
    buffer: Buffer
): Promise<{ mime: string; ext: string } | undefined> {
    const result = await FileType.fromBuffer(buffer)
    if (!result) return undefined
    return { mime: result.mime, ext: result.ext }
}

export function randomFileName(fileName: string): string {
    const extension = fileName.includes('.')
        ? '.' + fileName.split('.').pop()
        : ''
    const timestamp = Date.now()

    const additionalRandom = Math.random().toString(36).substring(2, 10)
    return `${timestamp}_${additionalRandom}${extension}`
}
