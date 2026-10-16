import test from 'node:test';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';

import { sha1sums } from '../src/utils/sha.js';

function sha1(text) {
  return createHash('sha1').update(text).digest('hex');
}

test('sha1sums hashes each array element followed by a newline', () => {
  assert.equal(sha1sums(['a', 'b']), sha1('a\nb\n'));
  assert.equal(sha1sums(['a', 'b']), '05dec960e24d918b8a73a1c53bcbbaac2ee5c2e0');
});

test('sha1sums hashes a single string as one line', () => {
  assert.equal(sha1sums('ab'), sha1('ab\n'));
  assert.equal(sha1sums('ab'), '5277cbb45a15902137d332d97e89cf8136545485');
  assert.equal(sha1sums('ab'), sha1sums(['ab']));
});

test('sha1sums of an empty array is the hash of the empty input', () => {
  assert.equal(sha1sums([]), 'da39a3ee5e6b4b0d3255bfef95601890afd80709');
});

test('sha1sums rejects non-string input', () => {
  assert.throws(() => sha1sums(123), TypeError);
  assert.throws(() => sha1sums(null), TypeError);
});
