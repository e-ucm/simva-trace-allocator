
/**
 * @typedef {object} DiffResult
 * @property {string[]} added
 * @property {string[]} removed
 */

/**
 * 
 * @param {string[]} a Ordered string array. 
 * @param {string[]} b Ordered string array.
 * 
 * @returns {DiffResult}
 */
export function diffArray(a, b) {
	/** @type {DiffResult} */
	const result = {
		added: [],
		removed: []
	};
	const minLength = Math.min(a.length, b.length);
	let idxA, idxB;
	for(idxA = 0, idxB=0; idxA < minLength;) {
		const aValue = a[idxA];
		const bValue = b[idxB];
		if (aValue === bValue) {
			idxA++;
			idxB++;
		} else if (aValue < bValue){
            result.removed.push(aValue);
            idxA++;
		} else {
            result.added.push(bValue);
            idxB++;
        }
	}
	if (idxA < a.length) {
		for(;idxA < a.length; idxA++) {
            const aValue = a[idxA];
			result.removed.push(aValue);
		}
	}
    if (idxB < b.length) {
		for(;idxB < b.length; idxB++) {
            const bValue = b[idxB];
			result.added.push(bValue);
		}
    }
	return result;
}

/**
 * 
 * @param {Set<string>} s1 
 * @param {Set<string>} s2 
 */
export function diffSet(s1, s2) {
	/** @type {DiffResult} */
	const result = {
		added: [],
		removed: []
	};

    for(const e of s1) {
        if (!s2.has(e)) {
            result.removed.push(e);
        }
    }

    for(const e of s2) {
        if (!s1.has(e)) {
            result.added.push(e);
        }
    }

    return result;
}

/**
 * @template T
 * @param {T[]} array 
 * @param {T} value
 * @param {boolean} findPosition
 * @param {(a:T, b:T) => number} [comparator]
 * 
 * @returns
 */
export function binarySearch(array, value, findPosition, comparator) {
    if (array.length === 0) return -1;

    comparator = comparator || ((a, b) => Number(a) - Number(b));
    return recursiveBinarySearch(array, value, 0, array.length, findPosition, comparator);
}

/**
 * @template T
 * @param {T[]} array 
 * @param {T} value
 * @param {number} start
 * @param {number} end
 * @param {boolean} findPosition
 * @param {(a:T, b:T) => number} [comparator]
 * 
 * @returns
 */
export function recursiveBinarySearch(array, value, start, end, findPosition, comparator) {
      
    // Base Condition
    if(findPosition) {
        if (start > end) return -start-1;
    } else {
        if (start > end) return -1;
    }
    
  
    // Find the middle index
    const mid = Math.floor((start + end)/2);
  
    // Compare mid with given key x
    const comparation = comparator(array[mid], value);
    
    if (comparation === 0) return mid;     
    if (comparation > 0) return recursiveBinarySearch(array, value, start, mid-1,findPosition, comparator);
    
    return recursiveBinarySearch(array, value, mid+1, end, findPosition, comparator);
}