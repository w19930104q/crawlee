import { rm } from 'node:fs/promises';
import path from 'node:path';
import { Writable } from 'node:stream';

import {
    Dataset,
    KeyValueStore,
    MemoryStorageBackend,
    serviceLocator,
    withDirectStorageAccess,
    withStorageTransaction,
} from '@crawlee/core';
import type { DatasetExportToStreamOptions } from '@crawlee/core';
import { FileSystemStorageBackend } from '@crawlee/fs-storage';
import type { DatasetBackend, Dictionary } from '@crawlee/types';

const fileRoot = path.resolve(import.meta.dirname, './tmp/export-to-stream');

let fileDirCounter = 0;

function dataItems(count: number) {
    return Array.from({ length: count }, (_, id) => ({ id, name: `item-${id}` }));
}

function getBackend(dataset: Dataset): DatasetBackend {
    // @ts-expect-error Accessing private property
    return dataset.backend;
}

async function streamToString(
    dataset: Dataset,
    options?: DatasetExportToStreamOptions,
    contentType?: string,
): Promise<{ output: string; writable: Writable }> {
    const chunks: Buffer[] = [];
    const writable = new Writable({
        write(chunk, _encoding, callback) {
            chunks.push(Buffer.from(chunk));
            callback();
        },
    });

    await dataset.exportToStream(writable, options, contentType);

    return { output: Buffer.concat(chunks).toString('utf8'), writable };
}

async function referenceCsv(dataset: Dataset): Promise<string> {
    const key = `reference-${Math.random().toString(36).slice(2)}`;
    await dataset.exportToCSV(key);

    return readRawRecord(key);
}

async function referenceJson(dataset: Dataset): Promise<string> {
    const key = `reference-${Math.random().toString(36).slice(2)}`;
    await dataset.exportToJSON(key);

    return readRawRecord(key);
}

async function readRawRecord(key: string): Promise<string> {
    const store = await KeyValueStore.open();
    const record = await store.getRecord(key);

    const value = record!.value;

    if (typeof value === 'string') return value;
    if (Buffer.isBuffer(value)) return value.toString('utf8');

    return new TextDecoder().decode(new Uint8Array(value));
}

async function csvStringify(rows: unknown[][]): Promise<string> {
    const { stringify } = await import('csv-stringify/sync');
    return stringify(rows as Parameters<typeof stringify>[0]);
}

for (const backendKind of ['memory', 'file'] as const) {
    describe(`Dataset.exportToStream with the ${backendKind} backend`, () => {
        let localDataDirectory: string | undefined;
        let datasetCounter = 0;

        beforeEach(() => {
            if (backendKind === 'memory') {
                serviceLocator.setStorageBackend(new MemoryStorageBackend());
            } else {
                localDataDirectory = path.resolve(fileRoot, `suite-${fileDirCounter++}`);
                serviceLocator.setStorageBackend(new FileSystemStorageBackend({ localDataDirectory }));
            }

            datasetCounter = 0;
        });

        afterEach(async () => {
            if (localDataDirectory) {
                await rm(localDataDirectory, { recursive: true, force: true });
            }
            localDataDirectory = undefined;
        });

        function openDataset<Data extends Dictionary = Dictionary>() {
            return Dataset.open<Data>({ name: `dataset-${datasetCounter++}` });
        }

        test('writes the whole dataset as a JSON array and leaves the stream open', async () => {
            const items = [...dataItems(3), { id: 3, name: 'item-3', nested: { list: [1, { deep: 'value' }] } }];
            const dataset = await openDataset();
            await dataset.pushData(items);

            const { output, writable } = await streamToString(dataset);

            expect(output).toBe(await referenceJson(dataset));
            expect(output).toBe(JSON.stringify(items, null, 2));
            expect(writable.writableEnded).toBe(false);
            expect(writable.destroyed).toBe(false);
            expect(writable.listenerCount('error')).toBe(0);
            expect(writable.listenerCount('close')).toBe(0);
            expect(writable.listenerCount('finish')).toBe(0);
            expect(writable.listenerCount('drain')).toBe(0);
        });

        test('handles an empty dataset in both formats', async () => {
            const dataset = await openDataset();

            const { output } = await streamToString(dataset);
            expect(output).toBe('[]');
            expect(output).toBe(await referenceJson(dataset));

            const { output: csv } = await streamToString(dataset, { format: 'csv' });
            expect(csv).toBe('');
        });

        test('respects offset, limit and desc', async () => {
            const items = dataItems(12);
            const dataset = await openDataset();
            await dataset.pushData(items);

            const { output } = await streamToString(dataset, { offset: 3, limit: 4 });
            expect(JSON.parse(output)).toEqual(items.slice(3, 7));

            const { output: withoutLimit } = await streamToString(dataset, { offset: 5 });
            expect(JSON.parse(withoutLimit)).toEqual(items.slice(5));

            const { output: descending } = await streamToString(dataset, { desc: true, offset: 2, limit: 5 });
            expect(JSON.parse(descending)).toEqual(items.slice().reverse().slice(2, 7));

            const { output: emptyRange } = await streamToString(dataset, { offset: 100, limit: 3 });
            expect(emptyRange).toBe('[]');
        });

        test('writes CSV identical to exportToCSV, including escaping', async () => {
            const items = [
                { id: 1, text: 'a,b' },
                { id: 2, text: 'q"z' },
                { id: 3, text: 'line\nbreak' },
            ];
            const dataset = await openDataset();
            await dataset.pushData(items);

            const expected = await referenceCsv(dataset);
            const { output } = await streamToString(dataset, { format: 'csv' });

            expect(output).toBe(expected);
        });

        test('accepts a content type as the third argument', async () => {
            const items = dataItems(3);
            const dataset = await openDataset();
            await dataset.pushData(items);

            const { output } = await streamToString(dataset, undefined, 'text/csv');

            expect(output).toBe(await referenceCsv(dataset));
        });

        test('collects CSV keys only from the frozen range', async () => {
            const items = [{ a: 1 }, { b: 2, c: 3 }, { d: 4, e: 5 }, { f: 6 }];
            const dataset = await openDataset();
            await dataset.pushData(items);

            const options = { format: 'csv' as const, offset: 1, limit: 2, collectAllKeys: true };
            const { output } = await streamToString(dataset, options);

            const selected = items.slice(1, 3);
            const keys = [...new Set(selected.flatMap(Object.keys))];
            const expected = await csvStringify([
                keys,
                ...selected.map((item) => keys.map((key) => item[key as keyof typeof item] ?? null)),
            ]);

            expect(output).toBe(expected);
            expect(output.split('\n')[0]).toBe('b,c,d,e');
        });

        test('uses the first item of the frozen range as the CSV header', async () => {
            const items = [{ a: 1 }, { b: 2 }, { c: 3 }];
            const dataset = await openDataset();
            await dataset.pushData(items);

            const { output } = await streamToString(dataset, { format: 'csv', offset: 1, limit: 1 });

            expect(output).toBe(await csvStringify([['b'], [items[1].b]]));
        });

        test('keeps CSV header and rows in the same descending range', async () => {
            const items = [{ a: 1 }, { b: 2, c: 3 }, { d: 4 }, { e: 5, f: 6 }];
            const dataset = await openDataset();
            await dataset.pushData(items);

            const options = { format: 'csv' as const, desc: true, limit: 2, collectAllKeys: true };
            const { output } = await streamToString(dataset, options);

            const selected = items.slice().reverse().slice(0, 2);
            const keys = [...new Set(selected.flatMap(Object.keys))];

            expect(output.split('\n')[0]).toBe('e,f,d');
            expect(output).toBe(
                await csvStringify([
                    keys,
                    ...selected.map((item) => keys.map((key) => (item as Dictionary)[key] ?? null)),
                ]),
            );
        });

        test('reads the range in bounded pages', async () => {
            const items = dataItems(25);
            const dataset = await openDataset();
            await dataset.pushData(items);

            const getData = vitest.spyOn(getBackend(dataset), 'getData');

            const { output } = await streamToString(dataset, { pageSize: 2 });
            expect(getData).toHaveBeenCalledTimes(Math.ceil(items.length / 2));
            expect(JSON.parse(output)).toEqual(items);

            getData.mockClear();
            await streamToString(dataset, { pageSize: 2, desc: true });
            expect(getData).toHaveBeenCalledTimes(Math.ceil(items.length / 2));

            getData.mockClear();
            await streamToString(dataset, { pageSize: 2, format: 'csv', collectAllKeys: true });
            // collectAllKeys needs a key-collection pass and a data pass over the same range.
            expect(getData).toHaveBeenCalledTimes(Math.ceil(items.length / 2) * 2);
        });

        test('waits for drain before reading the next page', async () => {
            const items = dataItems(6);
            const dataset = await openDataset();
            await dataset.pushData(items);

            const events: string[] = [];
            let activeWrites = 0;
            let maxActiveWrites = 0;

            const writable = new Writable({
                highWaterMark: 1,
                write(_chunk, _encoding, callback) {
                    activeWrites++;
                    maxActiveWrites = Math.max(maxActiveWrites, activeWrites);
                    events.push('write-start');
                    setTimeout(() => {
                        activeWrites--;
                        events.push('write-end');
                        callback();
                    }, 5);
                },
            });
            writable.on('drain', () => events.push('drain'));

            const backend = getBackend(dataset);
            const original = backend.getData.bind(backend);
            let reads = 0;
            vitest.spyOn(backend, 'getData').mockImplementation(async (options) => {
                reads++;
                events.push(`read-${reads}`);
                return original(options);
            });

            await dataset.exportToStream(writable, { pageSize: 2 });

            expect(reads).toBe(3);
            expect(maxActiveWrites).toBe(1);

            const readIndexes = events.flatMap((event, index) => (event.startsWith('read-') ? [index] : []));
            for (let i = 1; i < readIndexes.length; i++) {
                expect(events.lastIndexOf('drain', readIndexes[i])).toBeGreaterThan(readIndexes[i - 1]);
            }
        });

        test('waits for drain when write() reports backpressure without a callback', async () => {
            const items = dataItems(4);
            const dataset = await openDataset();
            await dataset.pushData(items);

            const chunks: string[] = [];
            const writable = new Writable();
            writable.write = ((chunk: string) => {
                chunks.push(chunk);
                queueMicrotask(() => writable.emit('drain'));
                return false;
            }) as typeof writable.write;

            await dataset.exportToStream(writable, { pageSize: 2 });

            expect(JSON.parse(chunks.join(''))).toEqual(items);
        });

        test('does not mix concurrently appended items into the ascending range', async () => {
            const items = dataItems(6);
            const dataset = await openDataset();
            await dataset.pushData(items);

            const backend = getBackend(dataset);
            const original = backend.getData.bind(backend);
            let appended = false;
            vitest.spyOn(backend, 'getData').mockImplementation(async (options) => {
                const page = await original(options);
                if (!appended) {
                    appended = true;
                    await dataset.pushData({ id: 100, name: 'late' });
                }
                return page;
            });

            const { output } = await streamToString(dataset, { pageSize: 2 });

            expect(JSON.parse(output)).toEqual(items);
            expect(await dataset.getInfo()).toMatchObject({ itemCount: items.length + 1 });
        });

        test('does not duplicate or skip items when appends happen during a descending export', async () => {
            const items = dataItems(6);
            const dataset = await openDataset();
            await dataset.pushData(items);

            const backend = getBackend(dataset);
            const original = backend.getData.bind(backend);
            let appended = false;
            vitest.spyOn(backend, 'getData').mockImplementation(async (options) => {
                const page = await original(options);
                if (!appended) {
                    appended = true;
                    await dataset.pushData({ id: 100, name: 'late' });
                }
                return page;
            });

            const { output } = await streamToString(dataset, { pageSize: 2, desc: true });

            expect(JSON.parse(output)).toEqual(items.slice().reverse());
        });

        test('rejects on a backend read failure and stops reading', async () => {
            const items = dataItems(6);
            const dataset = await openDataset();
            await dataset.pushData(items);

            const backend = getBackend(dataset);
            const original = backend.getData.bind(backend);
            let calls = 0;
            vitest.spyOn(backend, 'getData').mockImplementation(async (options) => {
                calls++;
                if (calls === 2) throw new Error('read exploded');
                return original(options);
            });

            const writable = new Writable({
                write(_chunk, _encoding, callback) {
                    callback();
                },
            });

            await expect(dataset.exportToStream(writable, { pageSize: 2 })).rejects.toThrow('read exploded');
            expect(calls).toBe(2);
            expect(writable.destroyed).toBe(false);
            expect(writable.listenerCount('error')).toBe(0);
            expect(writable.listenerCount('close')).toBe(0);
        });

        test('rejects on a serialization failure and stops reading', async () => {
            const dataset = await openDataset();
            await dataset.pushData({ id: 0 });

            const circular: Dictionary = {};
            circular.self = circular;

            const backend = getBackend(dataset);
            vitest.spyOn(backend, 'getData').mockResolvedValue({
                items: [circular],
                count: 1,
                total: 1,
                offset: 0,
                limit: 1,
                desc: false,
            });

            const writable = new Writable({
                write(_chunk, _encoding, callback) {
                    callback();
                },
            });

            await expect(dataset.exportToStream(writable)).rejects.toThrow(/circular/i);
            expect(writable.listenerCount('error')).toBe(0);
        });

        test('rejects on a stream write failure and releases listeners', async () => {
            const dataset = await openDataset();
            await dataset.pushData(dataItems(3));

            const writable = new Writable({
                write(_chunk, _encoding, callback) {
                    callback(new Error('write exploded'));
                },
            });

            await expect(dataset.exportToStream(writable, { pageSize: 2 })).rejects.toThrow('write exploded');
            expect(writable.listenerCount('error')).toBe(0);
            expect(writable.listenerCount('close')).toBe(0);
            expect(writable.listenerCount('finish')).toBe(0);
        });

        test('rejects when the stream closes early and stops reading', async () => {
            const items = dataItems(6);
            const dataset = await openDataset();
            await dataset.pushData(items);

            const chunks: Buffer[] = [];
            const writable = new Writable({
                write(chunk, _encoding, callback) {
                    chunks.push(Buffer.from(chunk));
                    callback();
                },
            });

            const backend = getBackend(dataset);
            const original = backend.getData.bind(backend);
            let calls = 0;
            vitest.spyOn(backend, 'getData').mockImplementation(async (options) => {
                calls++;
                if (calls === 1) writable.destroy();
                return original(options);
            });

            await expect(dataset.exportToStream(writable, { pageSize: 2 })).rejects.toThrow(/closed or ended/);
            expect(calls).toBe(1);
            expect(writable.listenerCount('error')).toBe(0);
            expect(writable.listenerCount('close')).toBe(0);
        });

        test('rejects on AbortSignal cancellation and stops reading', async () => {
            const items = dataItems(6);
            const dataset = await openDataset();
            await dataset.pushData(items);

            const controller = new AbortController();
            const writable = new Writable({
                write(_chunk, _encoding, callback) {
                    callback();
                },
            });

            const backend = getBackend(dataset);
            const original = backend.getData.bind(backend);
            let calls = 0;
            vitest.spyOn(backend, 'getData').mockImplementation(async (options) => {
                calls++;
                const page = await original(options);
                if (calls === 1) controller.abort();
                return page;
            });

            await expect(dataset.exportToStream(writable, { pageSize: 2, signal: controller.signal })).rejects.toThrow(
                /aborted/i,
            );

            expect(calls).toBe(1);
            expect(writable.listenerCount('error')).toBe(0);
            expect(writable.destroyed).toBe(false);
        });

        test('rejects a pre-aborted signal before reading', async () => {
            const dataset = await openDataset();
            await dataset.pushData(dataItems(2));

            const controller = new AbortController();
            controller.abort();

            const getData = vitest.spyOn(getBackend(dataset), 'getData');
            const writable = new Writable({
                write(_chunk, _encoding, callback) {
                    callback();
                },
            });

            await expect(dataset.exportToStream(writable, { signal: controller.signal })).rejects.toThrow(/aborted/i);

            expect(getData).not.toHaveBeenCalled();
        });

        test('rejects inside a storage transaction before reading or writing', async () => {
            const dataset = await openDataset();
            await dataset.pushData(dataItems(3));

            const getData = vitest.spyOn(getBackend(dataset), 'getData');
            const writable = new Writable({
                write(_chunk, _encoding, callback) {
                    callback();
                },
            });

            await expect(
                withStorageTransaction(async () => dataset.exportToStream(writable, { pageSize: 2 })),
            ).rejects.toThrow(/cannot be used inside a storage transaction/);

            expect(getData).not.toHaveBeenCalled();
            expect(writable.listenerCount('error')).toBe(0);
        });

        test('works inside a transaction through withDirectStorageAccess', async () => {
            const items = dataItems(3);
            const dataset = await openDataset();
            await dataset.pushData(items);

            const { output } = await withStorageTransaction(async () =>
                withDirectStorageAccess(async () => streamToString(dataset, { pageSize: 2 })),
            );

            expect(JSON.parse(output)).toEqual(items);
        });
    });
}
