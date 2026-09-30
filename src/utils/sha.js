import { createHash } from 'node:crypto';

/**
 * @overload
 * 
 * @param {string} str 
 * 
 * @returns {string}
 */
/**
 * @overload
 * 
 * @param {string[]} strings 
 * 
 * @returns {string}
 */
/**
 * 
 * @param {string | string[]} strings 
 * 
 * @returns {string}
 */
export function sha1sums(strings) {
    /** @type {string[]} */
    let str;
    if (Array.isArray(strings)) {
        str = strings;
    } else if (typeof strings === 'string') {
        str = [ strings ]
    } else {
        throw new TypeError(`'strings' must be string or string[]`);
    }

    const hash = createSha1();
    for(const s of strings) {
        hash.update(s);
        hash.update('\n');
    }
    const sha1 = hash.digest('hex');
    return sha1;
}


/**
 * Create sha1 hash function.
 *
 * @returns 
 */
function createSha1() {
	return createHash('sha1');
}
