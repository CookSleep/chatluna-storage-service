import { createHash } from 'crypto'

const FILE_MIME_TYPES: Record<string, string> = {
    mp4: 'video/mp4',
    webm: 'video/webm',
    mov: 'video/quicktime',
    mp3: 'audio/mpeg',
    m4a: 'audio/mp4',
    wav: 'audio/wav',
    ogg: 'audio/ogg'
}

/**
 * Computes the SHA-256 hash of the given buffer and returns it as a hex string.
 * SHA-256 is cryptographically modern and collision-resistant.
 */
export function computeHash(buffer: Buffer): string {
    return createHash('sha256').update(buffer).digest('hex')
}

export function getImageType(
    buffer: Buffer,
    pure: boolean = false,
    checkIsImage: boolean = true
): string {
    if (buffer.length < 12) {
        return checkIsImage ? undefined : pure ? 'jpg' : 'image/jpeg'
    }

    // PNG: 89 50 4E 47 0D 0A 1A 0A
    if (
        buffer[0] === 0x89 &&
        buffer[1] === 0x50 &&
        buffer[2] === 0x4e &&
        buffer[3] === 0x47 &&
        buffer[4] === 0x0d &&
        buffer[5] === 0x0a &&
        buffer[6] === 0x1a &&
        buffer[7] === 0x0a
    ) {
        return pure ? 'png' : 'image/png'
    }

    // JPEG: FF D8 FF
    if (buffer[0] === 0xff && buffer[1] === 0xd8 && buffer[2] === 0xff) {
        return pure ? 'jpg' : 'image/jpeg'
    }

    // GIF: 47 49 46 38 (GIF8)
    if (
        buffer[0] === 0x47 &&
        buffer[1] === 0x49 &&
        buffer[2] === 0x46 &&
        buffer[3] === 0x38
    ) {
        return pure ? 'gif' : 'image/gif'
    }

    // WebP: 52 49 46 46 ... 57 45 42 50 (RIFF....WEBP)
    if (
        buffer[0] === 0x52 &&
        buffer[1] === 0x49 &&
        buffer[2] === 0x46 &&
        buffer[3] === 0x46 &&
        buffer[8] === 0x57 &&
        buffer[9] === 0x45 &&
        buffer[10] === 0x42 &&
        buffer[11] === 0x50
    ) {
        return pure ? 'webp' : 'image/webp'
    }

    if (checkIsImage) {
        return undefined
    }

    return pure ? 'jpg' : 'image/jpeg'
}


export function randomFileName(fileName: string): string {
    const extension = fileName.includes('.')
        ? '.' + fileName.split('.').pop()
        : ''
    const timestamp = Date.now()

    const additionalRandom = Math.random().toString(36).substring(2, 10)
    return `${timestamp}_${additionalRandom}${extension}`
}

export function getMimeTypeFromFilename(fileName: string): string | undefined {
    return FILE_MIME_TYPES[fileName.toLowerCase().split('.').pop() ?? '']
}
