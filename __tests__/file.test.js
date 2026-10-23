import test from 'node:test';
import assert from 'node:assert/strict';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { writeFile, mkdir, readFile } from 'node:fs/promises';

import {
  mktempPath,
  mktemp,
  fileExists,
  directoryExists,
  ensureDirectoryStructureExists,
  forceRemove,
  listFiles,
  copyNoOverwrite,
  rename,
  withFile,
  createSandbox
} from '../src/utils/file.js';

test('mktempPath builds a random path in the requested directory', () => {
  const path = mktempPath('pre.', '.post', tmpdir());
  assert.ok(path.startsWith(join(tmpdir(), 'pre.')));
  assert.ok(path.endsWith('.post'));

  const defaulted = mktempPath();
  assert.ok(defaulted.startsWith(join(tmpdir(), 'tmp.')));
});

test('createSandbox provides a temporary directory and cleans it up', async () => {
  const sandbox = createSandbox('simva-test-');

  assert.equal(await directoryExists(sandbox.directoryPath), false);

  let inside;
  await sandbox.withinSandbox(async ({ directoryPath }) => {
    inside = directoryPath;
    assert.equal(await directoryExists(directoryPath), true);
  });

  assert.equal(inside, sandbox.directoryPath);
  assert.equal(await directoryExists(sandbox.directoryPath), false);
});

test('mktemp creates a file', async () => {
  const sandbox = createSandbox('simva-test-');

  await sandbox.withinSandbox(async ({ directoryPath }) => {
    const handle = await mktemp('f.', '.txt', directoryPath);
    assert.ok(handle);
    await handle.close();

    const files = await listFiles(directoryPath);
    assert.equal(files.length, 1);
    assert.ok(files[0].startsWith('f.'));
    assert.ok(files[0].endsWith('.txt'));
  });
});

test('fileExists and directoryExists distinguish paths', async () => {
  const sandbox = createSandbox('simva-test-');

  await sandbox.withinSandbox(async ({ directoryPath }) => {
    const file = join(directoryPath, 'a.txt');
    await writeFile(file, 'hi');

    assert.equal(await fileExists(file), true);
    assert.equal(await fileExists(join(directoryPath, 'nope.txt')), false);
    assert.equal(await directoryExists(directoryPath), true);
    assert.equal(await directoryExists(join(directoryPath, 'nope')), false);
  });
});

test('ensureDirectoryStructureExists creates nested directories', async () => {
  const sandbox = createSandbox('simva-test-');

  await sandbox.withinSandbox(async ({ directoryPath }) => {
    const nested = join(directoryPath, 'a', 'b', 'c');
    await ensureDirectoryStructureExists(nested);
    assert.equal(await directoryExists(nested), true);
  });
});

test('listFiles returns only the files of a directory', async () => {
  const sandbox = createSandbox('simva-test-');

  await sandbox.withinSandbox(async ({ directoryPath }) => {
    await writeFile(join(directoryPath, 'one.txt'), '1');
    await writeFile(join(directoryPath, 'two.txt'), '2');
    await mkdir(join(directoryPath, 'sub'));

    assert.deepEqual((await listFiles(directoryPath)).sort(), ['one.txt', 'two.txt']);
  });
});

test('copyNoOverwrite copies once and refuses to overwrite', async () => {
  const sandbox = createSandbox('simva-test-');

  await sandbox.withinSandbox(async ({ directoryPath }) => {
    const source = join(directoryPath, 'source.txt');
    const dest = join(directoryPath, 'dest.txt');
    await writeFile(source, 'data');

    await copyNoOverwrite(source, dest);
    assert.equal(await readFile(dest, 'utf8'), 'data');

    await assert.rejects(() => copyNoOverwrite(source, dest), /Could not copy/);
  });
});

test('rename moves a file with both rename and copy strategies', async () => {
  const sandbox = createSandbox('simva-test-');

  await sandbox.withinSandbox(async ({ directoryPath }) => {
    const source = join(directoryPath, 'a.txt');
    await writeFile(source, 'data');

    const renamed = join(directoryPath, 'b.txt');
    await rename(source, renamed);
    assert.equal(await fileExists(source), false);
    assert.equal(await readFile(renamed, 'utf8'), 'data');

    const copied = join(directoryPath, 'c.txt');
    await rename(renamed, copied, true);
    assert.equal(await fileExists(renamed), false);
    assert.equal(await readFile(copied, 'utf8'), 'data');
  });
});

test('forceRemove removes entries and tolerates missing ones', async () => {
  const sandbox = createSandbox('simva-test-');

  await sandbox.withinSandbox(async ({ directoryPath }) => {
    const file = join(directoryPath, 'a.txt');
    await writeFile(file, 'x');

    await forceRemove(file);
    assert.equal(await fileExists(file), false);

    await forceRemove(join(directoryPath, 'missing'));
  });
});

test('withFile opens a file, and can ignore open errors', async () => {
  const sandbox = createSandbox('simva-test-');

  await sandbox.withinSandbox(async ({ directoryPath }) => {
    const file = join(directoryPath, 'a.txt');
    await writeFile(file, 'abc');

    const opened = withFile(file, 'r');
    assert.equal(await opened((handle) => handle.readFile('utf8')), 'abc');

    const missing = withFile(join(directoryPath, 'nope.txt'), 'r');
    assert.equal(await missing(async () => 'never', false), undefined);
    await assert.rejects(() => missing(async () => 'never', true), /Could not open file/);
  });
});
