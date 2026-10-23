import test from 'node:test';
import assert from 'node:assert/strict';

import { isObject, isErrorWithCode, NotFoundError, wrapError } from '../src/utils/errors.js';

test('isObject only accepts plain non-array objects', () => {
  assert.equal(isObject({}), true);
  assert.equal(isObject(new Date()), true);
  assert.equal(isObject([]), false);
  assert.equal(isObject(null), false);
  assert.equal(isObject('x'), false);
  assert.equal(isObject(1), false);
});

test('isErrorWithCode detects errors carrying a code', () => {
  const error = new Error('boom');
  error.code = 'ENOENT';
  assert.equal(isErrorWithCode(error), true);
  assert.equal(isErrorWithCode({}), false);
  assert.equal(isErrorWithCode(new Error('plain')), false);
  assert.equal(isErrorWithCode(null), false);
});

test('NotFoundError keeps its name and message', () => {
  const error = new NotFoundError('file missing');
  assert.ok(error instanceof Error);
  assert.equal(error.name, 'NotFoundError');
  assert.equal(error.message, 'file missing');
});

test('wrapError wraps an Error and preserves the cause', () => {
  const original = new Error('boom');
  const wrapped = wrapError(original, 'context');

  assert.equal(wrapped.message, 'context');
  assert.equal(wrapped.cause, original);
});

test('wrapError copies the code of the original error', () => {
  const original = new Error('boom');
  original.code = 'ENOENT';

  assert.equal(wrapError(original, 'context').code, 'ENOENT');
});

test('wrapError stringifies non-error values', () => {
  assert.equal(wrapError('boom', 'context').message, 'boom: context');
  assert.equal(wrapError('boom', '').message, 'boom');
  assert.equal(wrapError(42, 'context').message, '42: context');
});
