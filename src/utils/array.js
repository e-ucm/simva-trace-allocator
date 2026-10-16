/**
 * Compare two arrays of strings. 
 * 
 * @param {string[]} arr1 
 * @param {string[]} arr2 
 */
export function areArraysEqual(arr1, arr2) {
    return arr1.length === arr2.length && arr1.every((val, index) => val === arr2[index]);
}

/**
 * Check if the given `value` is an array of strings. 
 * 
 * @param {Object} value 
 */
export function isStringArray(value) {
    return Array.isArray(value) && value.every(item => typeof item === "string");
}

/**
 * Print the first and last `x` elements of an array.
 * 
 * @param {string[]} arr 
 * @param {number} x 
 */
export function getFirstAndLastX(arr, x) {
    if (arr.length <= 2 * x) {
        return arr; // If the array is small, print it all
    }

    const firstX = arr.slice(0, x); // First X elements
    const lastX = arr.slice(-x); // Last X elements

    return [...firstX, "...", ...lastX]; // Using "..." as a placeholder
}