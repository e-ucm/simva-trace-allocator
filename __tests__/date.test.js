import test from 'node:test';
import assert from 'node:assert/strict';

import {
  now,
  epoch,
  parseDate,
  duration,
  formatDuration,
  convertTimeToCron
} from '../src/utils/date.js';

const DAY = 24 * 60 * 60 * 1000;

test('now returns the current date', () => {
  const before = Date.now();
  const value = now();
  assert.ok(value instanceof Date);
  assert.ok(value.getTime() >= before);
});

test('epoch returns the unix epoch', () => {
  assert.equal(epoch().getTime(), 0);
});

test('parseDate parses an ISO string', () => {
  const value = parseDate('2020-01-01T00:00:00.000Z');
  assert.ok(value instanceof Date);
  assert.equal(value.getTime(), Date.parse('2020-01-01T00:00:00.000Z'));
});

test('duration is inclusive of the last millisecond', () => {
  assert.equal(duration(new Date(0), new Date(1000)), 1001);
});

test('formatDuration serializes a duration', () => {
  assert.equal(formatDuration(0), 'PT0S');
  assert.equal(formatDuration(1000), 'PT1S');
  assert.equal(formatDuration(60 * 1000), 'PT1M');
  assert.equal(formatDuration(60 * 60 * 1000), 'PT1H');
  assert.equal(formatDuration(DAY), 'P1D');
  assert.equal(formatDuration(30 * DAY), 'P1M');
  assert.equal(formatDuration(360 * DAY), 'P1Y');
  assert.equal(formatDuration(3661000), 'PT1H1M1S');
});

test('convertTimeToCron rejects non-positive durations', () => {
  assert.throws(() => convertTimeToCron(0), /greater than 0/);
  assert.throws(() => convertTimeToCron(-1), /greater than 0/);
});

test('convertTimeToCron uses minute precision under an hour', () => {
  assert.equal(convertTimeToCron(30), '*/30 * * * *');
});

test('convertTimeToCron uses hours under a day', () => {
  assert.equal(convertTimeToCron(60), '0 */1 * * *');
  assert.equal(convertTimeToCron(90), '0 */1 * * *\n30 */1 * * *');
});

test('convertTimeToCron uses days under a week', () => {
  assert.equal(convertTimeToCron(1440), '0 0 */1 * *');
});

test('convertTimeToCron uses weeks and months for long durations', () => {
  assert.equal(convertTimeToCron(10080), '0 0 * */1 *');
  assert.equal(convertTimeToCron(43200), '0 0 1 */1 *');
});
