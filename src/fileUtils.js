import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { open } from 'node:fs/promises';
import { randomBytes } from 'node:crypto';

/**
 * 
 * @param {string} [prefix]
 * @param {string} [suffix] 
 * @param {string} [tmpDirPath]
 * 
 * @returns {Promise<string>}
 */
export async function mktempPath(prefix, suffix, tmpDirPath) {
    prefix = prefix !== undefined ? prefix : 'tmp.';
    suffix = suffix !== undefined? suffix : '';
    tmpDirPath = tmpDirPath !== undefined ? tmpDirPath : tmpdir();
    const random = randomBytes(8).toString('base64url');
    const tmpPath = join(tmpDirPath, `${prefix}${random}${suffix}`);
    return tmpPath;
}

/**
 * 
 * @param {string} [prefix]
 * @param {string} [suffix] 
 * @param {string} [tmpDirPath]
 */
export async function mktemp(prefix, suffix, tmpDirPath) {
    const tmpPath = await mktempPath(prefix, suffix, tmpDirPath);
    const tmpFile = await open(tmpPath, 'wx');
    return tmpFile;
}

