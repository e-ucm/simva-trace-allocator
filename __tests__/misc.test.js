import test from 'node:test';
import assert from 'node:assert/strict';

import { diffArray, diffSet, binarySearch, recursiveBinarySearch } from '../src/utils/misc.js';

test('diffArray reports both sides of the diff', () => {
  assert.deepEqual(diffArray(['a', 'b', 'c'], ['a', 'c']), { added: [], removed: ['b'] });
  assert.deepEqual(diffArray(['a', 'c'], ['a', 'b', 'c']), { added: ['b'], removed: [] });
  assert.deepEqual(diffArray(['a', 'b', 'c'], ['a']), { added: [], removed: ['b', 'c'] });
  assert.deepEqual(diffArray(['a', 'x'], ['a', 'b', 'c']), { added: ['b', 'c'], removed: ['x'] });
  assert.deepEqual(diffArray(['a', 'b'], ['a', 'b']), { added: [], removed: [] });
});

test('diffSet reports set differences', () => {
  assert.deepEqual(diffSet(new Set(['a', 'b']), new Set(['b', 'c'])), { added: ['c'], removed: ['a'] });
  assert.deepEqual(diffSet(new Set(), new Set()), { added: [], removed: [] });
});

test('binarySearch finds and reports missing values', () => {
  assert.equal(binarySearch([], 1, false), -1);
  assert.equal(binarySearch([1, 2, 3, 4, 5], 3, false), 2);
  assert.equal(binarySearch([1, 2, 3, 4, 5], 42, false), -1);
});

test('binarySearch reports the insertion position when asked', () => {
  assert.equal(binarySearch([1, 3, 5], 2, true), -2);
  assert.equal(binarySearch([1, 3, 5], 0, true), -1);
  assert.equal(binarySearch([1, 3, 5], 6, true), -5);
});

test('binarySearch accepts a custom comparator', () => {
  const array = [{ v: 1 }, { v: 3 }, { v: 5 }];
  assert.equal(binarySearch(array, { v: 3 }, false, (a, b) => a.v - b.v), 1);
});

test('recursiveBinarySearch is exposed and consistent', () => {
  assert.equal(recursiveBinarySearch([1, 2, 3], 3, 0, 3, false, (a, b) => a - b), 2);
});
