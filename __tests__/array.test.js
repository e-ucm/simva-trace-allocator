import test from 'node:test';
import assert from 'node:assert/strict';

import { areArraysEqual, isStringArray, getFirstAndLastX } from '../src/utils/array.js';

test('areArraysEqual compares length and order', () => {
  assert.equal(areArraysEqual([], []), true);
  assert.equal(areArraysEqual(['a', 'b'], ['a', 'b']), true);
  assert.equal(areArraysEqual(['a', 'b'], ['b', 'a']), false);
  assert.equal(areArraysEqual(['a'], ['a', 'b']), false);
});

test('isStringArray accepts empty and string-only arrays', () => {
  assert.equal(isStringArray([]), true);
  assert.equal(isStringArray(['a', 'b']), true);
  assert.equal(isStringArray(['a', 1]), false);
  assert.equal(isStringArray('a'), false);
  assert.equal(isStringArray(null), false);
});

test('getFirstAndLastX returns the array untouched when it is short', () => {
  const arr = [1, 2, 3];
  assert.equal(getFirstAndLastX(arr, 2), arr);
});

test('getFirstAndLastX truncates the middle of long arrays', () => {
  assert.deepEqual(getFirstAndLastX([1, 2, 3, 4, 5, 6, 7, 8], 2), [1, 2, '...', 7, 8]);
});
