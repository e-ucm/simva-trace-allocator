import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { constants, copyFile, mkdir, open, readdir, rm, stat, unlink, rename as nodeRename } from 'node:fs/promises';
import { randomBytes } from 'node:crypto';
import { promisify } from 'node:util';
import { isErrorWithCode, wrapError } from './errors.js';

// Some of the functions are adapted from https://github.com/MetaMask/utils/blob/main/src/fs.ts

const randomBytesAsync = promisify(randomBytes);

/**
 * Builds a path for a temporal file.
 * 
 * @param {string} [prefix]
 * @param {string} [suffix] 
 * @param {string} [tmpDirPath]
 * 
 * @returns {string}
 */
export function mktempPath(prefix, suffix, tmpDirPath) {
    prefix = prefix ?? 'tmp.';
    suffix = suffix ?? '';
    tmpDirPath = tmpDirPath ?? tmpdir();
    const random = randomBytes(8).toString('base64url');
    const tmpPath = join(tmpDirPath, `${prefix}${random}${suffix}`);
    return tmpPath;
}

const MAX_RETRIES = 5;

/**
 * Creates a temp file.
 * 
 * @param {string} [prefix]
 * @param {string} [suffix] 
 * @param {string} [tmpDirPath]
 */
export async function mktemp(prefix, suffix, tmpDirPath) {
    let tmpFile;
    let attempt = 0;
    while (tmpFile === undefined && attempt < MAX_RETRIES) {
        try {
            const tmpPath = mktempPath(prefix, suffix, tmpDirPath);
            tmpFile = await open(tmpPath, 'wx');
        } catch (e) {
            attempt++;
        }
    }
    if (tmpFile === undefined) {
        throw new Error(`Could not create temp path`);
    }
    return tmpFile;
}

/**
 * Test the given path to determine whether it represents a file.
 *
 * @param {string} filePath - The path to a (supposed) file on the filesystem.
 * @returns {Promise<boolean>} A promise for true if the file exists or false otherwise.
 * @throws An error with a stack trace if reading fails in any way.
 */
export async function fileExists(filePath) {
    try {
        const stats = await stat(filePath);
        return stats.isFile();
    } catch (error) {
        if (isErrorWithCode(error) && error.code === 'ENOENT') {
            return false;
        }

        throw wrapError(error, `Could not determine if file exists '${filePath}'`);
    }
}

/**
 * Test the given path to determine whether it represents a directory.
 *
 * @param {string} directoryPath - The path to a (supposed) directory on the filesystem.
 * @returns {Promise<boolean>} A promise for true if the file exists or false otherwise.
 * @throws An error with a stack trace if reading fails in any way.
 */
export async function directoryExists(directoryPath) {
    try {
        const stats = await stat(directoryPath);
        return stats.isDirectory();
    } catch (error) {
        if (isErrorWithCode(error) && error.code === 'ENOENT') {
            return false;
        }

        throw wrapError(
            error,
            `Could not determine if directory exists '${directoryPath}'`,
        );
    }
}

/**
 * Create the given directory along with any directories leading up to the
 * directory, or do nothing if the directory already exists.
 *
 * @param {string} directoryPath - The path to the desired directory.
 * @throws An error with a stack trace if reading fails in any way.
 */
export async function ensureDirectoryStructureExists(directoryPath) {
    try {
        await mkdir(directoryPath, { recursive: true });
    } catch (error) {
        throw wrapError(
            error,
            `Could not create directory structure '${directoryPath}'`,
        );
    }
}

/**
 * Remove the given file or directory if it exists, or do nothing if it does
 * not.
 *
 * @param entryPath - The path to the file or directory.
 * @throws An error with a stack trace if removal fails in any way.
 */
export async function forceRemove(entryPath) {
    try {
        await rm(entryPath, {
            recursive: true,
            force: true,
        });
    } catch (error) {
        throw wrapError(error, `Could not remove file or directory '${entryPath}'`);
    }
}

/**
 * Information about the file sandbox provided to tests that need temporary
 * access to the filesystem.
 * 
 * @typedef FileSandbox
 * @property {string} directoryPath
 * @property {(test: (args: { directoryPath: string }) => Promise<void>)=>Promise<void>} withinSandbox
 */

/**
 * Construct a sandbox object which can be used in tests that need temporary
 * access to the filesystem.
 *
 * @param {string} projectName - The name of the project.
 * @returns {FileSandbox} The sandbox object. This contains a `withinSandbox` function which
 * can be used in tests (see example).
 * @example
 * ```typescript
 * const { withinSandbox } = createSandbox('utils');
 *
 * // ... later ...
 *
 * it('does something with the filesystem', async () => {
 *   await withinSandbox(async ({ directoryPath }) => {
 *     await fs.promises.writeFile(
 *       path.join(directoryPath, 'some-file'),
 *       'some content',
 *       'utf8'
 *     );
 *   })
 * });
 * ```
 */
export function createSandbox(projectName) {
    const directoryPath = mktempPath(projectName);

    return {
        directoryPath,
        async withinSandbox(test) {
            if (await directoryExists(directoryPath)) {
                throw new Error(`${directoryPath} already exists. Cannot continue.`);
            }

            await ensureDirectoryStructureExists(directoryPath);

            try {
                await test({ directoryPath });
            } finally {
                await forceRemove(directoryPath);
            }
        },
    };
}


/**
 * List files in `path` directory.
 * 
 * @param {string } path 
 * @returns {Promise<string[]>}
 */
export async function listFiles(path) {
    try {
        const files = [];
        const entries = await readdir(path,  { withFileTypes: true});
        for(const entry of entries) {
            if (entry.isFile()) {
                files.push(entry.name);
            }
        }
        return files;
    } catch (error) {
        throw wrapError(error, `Could not list directory: ${path}`);
    }
}

/** @typedef {import('node:fs').PathLike} PathLike */
/** @typedef {import('node:fs').Mode} Mode */
/** @typedef {import('node:fs/promises').FileHandle} FileHandle */
/**
 * @param {PathLike} path 
 * @param {string | number} [flags]
 * @param {Mode} [mode] 
 */
export function withFile(path, flags, mode) {
    /**
     * @template T
     * @param {(file:FileHandle)=>Promise<T>} fn 
     * @param {boolean} [errorOnOpen=true] 
     * @returns {Promise<T>}
     */
    return async function withFileOp(fn, errorOnOpen=true) {
        let file;
        try {
            file = await open(path, flags, mode);
        } catch (error) {
            if (errorOnOpen) {  
                throw wrapError(`Could not open file: ${path}`, error);
            }
            return;
        }
        try {
            return await fn(file);
        } finally {
            await file.close();
        }
    }
}

/**
 * Copy `source` to `dest` but no overwrite `dest` if already exists. 
 * 
 * @param {string} source 
 * @param {string} dest 
 */
export async function copyNoOverwrite(source, dest) {
    try {
        return copyFile(source, dest, constants.COPYFILE_EXCL);
    } catch (error) {
        throw wrapError(error, `Could not copy file '${source}' to '${dest}'`);
    }
}

/**
 * Rename `source` to `dest`.
 * If `useCopy` is true instead of using `node:fs#rename`, `source` is copied
 * and later removed. This avoid some limitations of `rename`.
 * 
 * 
 * @param {*} source 
 * @param {*} dest 
 * @param {boolean} [useCopy=false]
 */
export async function rename(source, dest, useCopy = false) {
    try {
        if (useCopy) {
            await copyFile(source, dest);
            return unlink(source);
        } else {
            return nodeRename(source, dest);    
        }
    } catch (error) {
        throw wrapError(error, `Could not rename '${source}' to '${dest}'`);
    }
}