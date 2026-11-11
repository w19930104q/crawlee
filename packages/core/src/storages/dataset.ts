import type { Writable } from 'node:stream';

import type { Awaitable, DatasetBackend, DatasetInfo, Dictionary, PaginatedList } from '@crawlee/types';
import { z } from 'zod';

import { tryCancel } from '@apify/timeout';
import { jsonStringifyExtended } from '@apify/utilities';

import { Configuration } from '../configuration.js';
import { serviceLocator } from '../service-locator.js';
import { parseArgument, schemas, validators } from '../validators.js';
import type { DatasetJournalEntry, JournalEntry } from './transaction.js';
import { activeStorageTransaction, rejectOperationInTransaction, snapshotValue } from './transaction.js';
import { KeyValueStore } from './key-value-store.js';
import type { DatasetStats } from './storage-stats.js';
import { StorageStatsTracker } from './storage-stats.js';
import type { StorageOpenOptions } from './utils.js';
import type { StorageIdentifier } from './storage-instance-manager.js';
import { resolveStorageIdentifier } from './storage-instance-manager.js';
import { createDualIterable, purgeDefaultStorages } from './utils.js';

const openOptionsSchema = z.strictObject({
    configuration: z.instanceof(Configuration).optional(),
    storageBackend: validators.storageBackend.optional(),
});

/** @internal */
export const DATASET_ITERATORS_DEFAULT_LIMIT = 10000;

/**
 * Validates that the given value is a plain JSON-serializable object
 * (not an array, not a primitive, not circular).
 *
 * @param item The value to validate.
 * @param index Optional index for error messages when validating inside an array.
 * @ignore
 */
export function assertJsonSerializable<T>(item: T, index?: number): void {
    const s = typeof index === 'number' ? ` at index ${index} ` : ' ';
    const isItemObject = item && typeof item === 'object' && !Array.isArray(item);

    if (!isItemObject) {
        throw new Error(`Data item${s}is not an object. You can push only objects into a dataset.`);
    }

    try {
        JSON.stringify(item);
    } catch (e) {
        const err = e as Error;
        throw new Error(`Data item${s}is not serializable to JSON.\nCause: ${err.message}`);
    }
}

export interface DatasetDataOptions {
    /**
     * Number of array elements that should be skipped at the start.
     * @default 0
     */
    offset?: number;

    /**
     * Maximum number of array elements to return.
     * @default 250000
     */
    limit?: number;

    /**
     * If `true` then the objects are sorted by `createdAt` in descending order.
     * Otherwise they are sorted in ascending order.
     * @default false
     */
    desc?: boolean;

    /**
     * An array of field names that will be included in the result. If omitted, all fields are included in the results.
     */
    fields?: string[];

    /**
     * Specifies a name of the field in the result objects that will be used to unwind the resulting objects.
     * By default, the results are returned as they are.
     */
    unwind?: string;

    /**
     * If `true` then the function returns only non-empty items and skips hidden fields (i.e. fields starting with `#` character).
     * Note that the `clean` parameter is a shortcut for `skipHidden: true` and `skipEmpty: true` options.
     * @default false
     */
    clean?: boolean;

    /**
     * If `true` then the function doesn't return hidden fields (fields starting with "#" character).
     * @default false
     */
    skipHidden?: boolean;

    /**
     * If `true` then the function doesn't return empty items.
     * Note that in this case the returned number of items might be lower than limit parameter and pagination must be done using the `limit` value.
     * @default false
     */
    skipEmpty?: boolean;
}

export interface DatasetExportOptions extends Omit<DatasetDataOptions, 'offset' | 'limit'> {
    /**
     * If true, includes all unique keys from all dataset items in the CSV export header.
     * If omitted or false, only keys from the first item are used.
     */
    collectAllKeys?: boolean;
}

export interface DatasetIteratorOptions extends Omit<
    DatasetDataOptions,
    'offset' | 'limit' | 'clean' | 'skipHidden' | 'skipEmpty'
> {
    /** @internal */
    offset?: number;

    /**
     * @default 10000
     * @internal
     */
    limit?: number;

    /** @internal */
    clean?: boolean;

    /** @internal */
    skipHidden?: boolean;

    /** @internal */
    skipEmpty?: boolean;

    /** @internal */
    format?: string;
}

export interface DatasetExportToOptions extends DatasetExportOptions {
    fromDataset?: string | StorageIdentifier;
    toKVS?: string | StorageIdentifier;
}

/**
 * Options for {@apilink Dataset.exportToStream}.
 */
export interface DatasetExportToStreamOptions extends DatasetExportOptions {
    /**
     * Number of items that should be skipped at the start of the export range. With `desc`, the offset
     * is counted from the newest item, matching the `offset` semantics of {@apilink Dataset.getData}.
     * @default 0
     */
    offset?: number;

    /**
     * Maximum number of items to export. When omitted, every remaining item in the frozen range is exported.
     * @default undefined
     */
    limit?: number;

    /**
     * Output format. `json` writes a JSON array, `csv` writes CSV. Defaults to `json`.
     * {@apilink DatasetExportToStreamOptions.contentType|contentType} takes precedence when both are set.
     */
    format?: 'json' | 'csv';

    /**
     * MIME type of the streamed output. Only `application/json` and `text/csv` are supported.
     * @default 'application/json'
     */
    contentType?: 'application/json' | 'text/csv';

    /**
     * Number of items fetched by a single backend read. Bounds the memory used while streaming.
     * @default 10000
     */
    pageSize?: number;

    /**
     * When aborted, the export rejects immediately and no further pages are read. The destination
     * stream is left untouched (it is neither ended nor destroyed).
     */
    signal?: AbortSignal;
}

/**
 * The `Dataset` class represents a store for structured data where each object stored has the same attributes,
 * such as online store products or real estate offers. You can imagine it as a table,
 * where each object is a row and its attributes are columns.
 * Dataset is an append-only storage - you can only add new records to it but you cannot modify or remove existing records.
 * Typically it is used to store crawling results.
 *
 * Do not instantiate this class directly, use the
 * {@apilink Dataset.open} function instead.
 *
 * `Dataset` stores its data either on local disk or in the Apify cloud,
 * depending on whether the `APIFY_LOCAL_STORAGE_DIR` or `APIFY_TOKEN` environment variables are set.
 *
 * If the `APIFY_LOCAL_STORAGE_DIR` environment variable is set, the data is stored in
 * the local directory in the following files:
 * ```
 * {APIFY_LOCAL_STORAGE_DIR}/datasets/{DATASET_ID}/{INDEX}.json
 * ```
 * Note that `{DATASET_ID}` is the name or ID of the dataset. The default dataset has ID: `default`,
 * unless you override it by setting the `APIFY_DEFAULT_DATASET_ID` environment variable.
 * Each dataset item is stored as a separate JSON file, where `{INDEX}` is a zero-based index of the item in the dataset.
 *
 * If the `APIFY_TOKEN` environment variable is set but `APIFY_LOCAL_STORAGE_DIR` not, the data is stored in the
 * [Apify Dataset](https://docs.apify.com/storage/dataset)
 * cloud storage. Note that you can force usage of the cloud storage also by passing the `forceCloud`
 * option to {@apilink Dataset.open} function,
 * even if the `APIFY_LOCAL_STORAGE_DIR` variable is set.
 *
 * **Example usage:**
 *
 * ```javascript
 * // Write a single row to the default dataset
 * await Dataset.pushData({ col1: 123, col2: 'val2' });
 *
 * // Open a named dataset
 * const dataset = await Dataset.open('some-name');
 *
 * // Write a single row
 * await dataset.pushData({ foo: 'bar' });
 *
 * // Write multiple rows
 * await dataset.pushData([
 *   { foo: 'bar2', col2: 'val2' },
 *   { col3: 123 },
 * ]);
 *
 * // Export the entirety of the dataset to one file in the key-value store
 * await dataset.exportToCSV('MY-DATA');
 * ```
 * @category Result Stores
 */
export class Dataset<Data extends Dictionary = Dictionary> {
    readonly id: string;
    readonly name?: string;
    // oxlint-disable-next-line crawlee/prefer-private-fields -- tests spy on the backend directly
    private readonly backend: DatasetBackend<Data>;

    readonly #statsTracker = new StorageStatsTracker<DatasetStats>({
        readCount: 0,
        writeCount: 0,
    });

    /**
     * @internal
     */
    constructor(
        options: DatasetOptions,
        readonly configuration = Configuration.getGlobalConfiguration(),
    ) {
        this.id = options.metadata.id;
        this.name = options.metadata.name;
        this.backend = options.backend;
    }

    /**
     * Backend-independent usage counters tracked for this dataset (read / write operations issued to
     * the underlying storage backend). Counted per backend call.
     */
    get stats(): DatasetStats {
        return this.#statsTracker.current;
    }

    /**
     * Stores an object or an array of objects to the dataset.
     * The function returns a promise that resolves when the operation finishes.
     * It has no result, but throws on invalid args or other errors.
     *
     * **IMPORTANT**: Make sure to use the `await` keyword when calling `pushData()`,
     * otherwise the crawler process might finish before the data is stored!
     *
     * @param data Object or array of objects containing data to be stored in the default dataset.
     *   The objects must be serializable to JSON.
     */
    async pushData(data: Data | Data[]): Promise<void> {
        tryCancel();

        const transaction = activeStorageTransaction();

        parseArgument(data, schemas.anyObject);

        // Normalize to array and validate each item
        const items = Array.isArray(data) ? data : [data];
        for (let i = 0; i < items.length; i++) {
            assertJsonSerializable(items[i], i);
        }

        if (transaction) {
            // One snapshot serves both the reads and the commit replay, so the two cannot disagree.
            transaction.recordJournalEntry({
                type: 'dataset',
                participant: this,
                storageId: this.id,
                items: snapshotValue(items),
                recordedAt: new Date(),
            });
            return;
        }

        this.#statsTracker.add('writeCount');
        await this.backend.pushData(items);
    }

    /**
     * Returns {@apilink DatasetContent} object holding the items in the dataset based on the provided parameters.
     */
    async getData(options: DatasetDataOptions = {}): Promise<DatasetContent<Data>> {
        try {
            return await this.#readPage(options);
        } catch (e) {
            const error = e as Error;
            if (error.message.includes('Cannot create a string longer than')) {
                throw new Error(
                    'dataset.getData(): The response is too large for parsing. You can fix this by lowering the "limit" option.',
                );
            }
            throw e;
        }
    }

    /**
     * The single transaction-aware page read all dataset read paths go through — both `getData()` and
     * the private `fetchPages()`. Returns the real page concatenated with the current transaction's
     * buffered items, with `offset` / `limit` / `desc` windowing applied across the concatenation.
     */
    async #readPage(options: DatasetDataOptions): Promise<DatasetContent<Data>> {
        const buffered = this.#bufferedJournalEntries()?.flatMap((entry) => entry.items as Data[]);

        // Every branch below hits the backend exactly once.
        this.#statsTracker.add('readCount');

        if (!buffered?.length) {
            return this.backend.getData(options);
        }

        const { offset = 0, limit, desc = false } = options;

        if (!desc) {
            const realPage = await this.backend.getData(options);

            // Buffered items sit past `realPage.total`, so the window bounds must come from that - not
            // from the page's shortfall, which `skipEmpty` produces without exhausting the real items.
            const bufferedStart = Math.max(0, offset - realPage.total);
            const bufferedEnd = limit === undefined ? buffered.length : Math.max(0, offset + limit - realPage.total);
            const items = [...realPage.items, ...buffered.slice(bufferedStart, bufferedEnd)];

            return {
                items,
                total: realPage.total + buffered.length,
                offset,
                // A caller that passed no limit wants everything, so the backend-reported `limit` is
                // passed through - backends are free to report a page size or a sentinel there.
                limit: limit ?? realPage.limit,
                count: items.length,
                desc,
            };
        }

        // Descending order: the buffered items are the newest, so they come first, reversed.
        const reversedBuffer = [...buffered].reverse();
        const fromBuffer =
            limit === undefined ? reversedBuffer.slice(offset) : reversedBuffer.slice(offset, offset + limit);
        const needed = limit === undefined ? Infinity : limit - fromBuffer.length;

        if (needed <= 0) {
            // The whole window is served from the buffer; only the real total is missing.
            const { itemCount } = await this.backend.getMetadata();
            return {
                items: fromBuffer,
                total: itemCount + buffered.length,
                offset,
                limit: limit!,
                count: fromBuffer.length,
                desc,
            };
        }

        const realPage = await this.backend.getData({
            ...options,
            offset: Math.max(0, offset - buffered.length),
            ...(limit === undefined ? {} : { limit: needed }),
        });

        return {
            items: [...fromBuffer, ...realPage.items],
            total: realPage.total + buffered.length,
            offset,
            limit: limit ?? realPage.limit,
            count: fromBuffer.length + realPage.items.length,
            desc,
        };
    }

    /** The active transaction's buffered writes to this dataset, derived from its journal. */
    #bufferedJournalEntries(): DatasetJournalEntry[] | undefined {
        const transaction = activeStorageTransaction();
        return transaction?.journal.filter(
            (entry): entry is DatasetJournalEntry => entry.type === 'dataset' && entry.participant === this,
        );
    }

    /** @internal */
    async commitJournalEntries(entries: JournalEntry[]): Promise<void> {
        const items: Data[] = [];

        for (const entry of entries) {
            if (entry.type === 'dataset') {
                items.push(...(entry.items as Data[]));
            }
        }

        // One backend call with all journaled items, in order - as close to atomic as the backend allows.
        // Straight to the backend: the items were validated and snapshotted at write time.
        if (items.length > 0) {
            this.#statsTracker.add('writeCount');
            await this.backend.pushData(items);
        }
    }

    /**
     * Returns all the data from the dataset. This will iterate through the whole dataset
     * via the `listItems()` client method, which gives you only paginated results.
     */
    async export(options: DatasetExportOptions = {}): Promise<Data[]> {
        tryCancel();

        const items: Data[] = [];

        for await (const page of this.#fetchPages(options)) {
            items.push(...page.items);
        }

        return items;
    }

    /**
     * Save the entirety of the dataset's contents into one file within a key-value store.
     *
     * @param key The name of the value to save the data in.
     * @param [options] An optional options object where you can provide the dataset and target KVS name.
     * @param [contentType] Only JSON and CSV are supported currently, defaults to JSON.
     */
    async exportTo(key: string, options?: DatasetExportToOptions, contentType?: string): Promise<Data[]> {
        const kvStore = await KeyValueStore.open(options?.toKVS ?? null, { configuration: this.configuration });
        const items = await this.export(options);

        if (contentType === 'text/csv') {
            // To handle empty dataset exports gracefully.
            if (items.length === 0) {
                await kvStore.setValue(key, '', { contentType });
                return items;
            }

            const keys = options?.collectAllKeys
                ? Array.from(new Set(items.flatMap(Object.keys)))
                : Object.keys(items[0]);

            const { stringify } = await import('csv-stringify/sync');

            const value = stringify([
                keys,
                ...items.map((item) => {
                    return keys.map((k) => item[k]);
                }),
            ]);
            await kvStore.setValue(key, value, { contentType });
            return items;
        }

        if (contentType === 'application/json') {
            await kvStore.setValue(key, items);
            return items;
        }

        throw new Error(`Unsupported content type: ${contentType}`);
    }

    /**
     * Save entire default dataset's contents into one JSON file within a key-value store.
     *
     * @param key The name of the value to save the data in.
     * @param [options] An optional options object where you can provide the target KVS name.
     */
    async exportToJSON(key: string, options?: Omit<DatasetExportToOptions, 'fromDataset'>) {
        await this.exportTo(key, options, 'application/json');
    }

    /**
     * Save entire default dataset's contents into one CSV file within a key-value store.
     *
     * @param key The name of the value to save the data in.
     * @param [options] An optional options object where you can provide the target KVS name.
     */
    async exportToCSV(key: string, options?: Omit<DatasetExportToOptions, 'fromDataset'>) {
        await this.exportTo(key, options, 'text/csv');
    }

    /**
     * Save entire default dataset's contents into one JSON file within a key-value store.
     *
     * @param key The name of the value to save the data in.
     * @param [options] An optional options object where you can provide the dataset and target KVS name.
     */
    static async exportToJSON(key: string, options?: DatasetExportToOptions) {
        tryCancel();

        const dataset = await this.open(options?.fromDataset);
        await dataset.exportToJSON(key, options);
    }

    /**
     * Save entire default dataset's contents into one CSV file within a key-value store.
     *
     * @param key The name of the value to save the data in.
     * @param [options] An optional options object where you can provide the dataset and target KVS name.
     */
    static async exportToCSV(key: string, options?: DatasetExportToOptions) {
        tryCancel();

        const dataset = await this.open(options?.fromDataset);
        await dataset.exportToCSV(key, options);
    }

    /**
     * Streams the dataset into a caller-provided Node.js `Writable` as a JSON array or CSV.
     *
     * The export range is frozen when the call starts: items appended while the stream is being
     * written are never included, and pagination advances over fixed indices, so a descending
     * export cannot duplicate or skip items. CSV headers are taken from the same frozen range as
     * the rows. Only one page of items (plus the set of CSV column names when `collectAllKeys` is
     * enabled) is held in memory.
     *
     * The destination stream is never ended or destroyed - the caller owns it. `write()` returning
     * `false` pauses the export until the stream emits `drain`. The returned promise rejects on
     * backend, serialization or stream errors, on early stream closure, and when the `signal` is
     * aborted; reading stops in all of those cases.
     *
     * This method cannot be used inside a storage transaction. Wrap it in
     * {@apilink withDirectStorageAccess} if you need to call it from a request handler.
     *
     * **Example usage**
     * ```javascript
     * import { createWriteStream } from 'node:fs';
     *
     * // Stream the first 1000 items, newest first, as JSON
     * await dataset.exportToStream(createWriteStream('results.json'), {
     *   offset: 0,
     *   limit: 1000,
     *   desc: true,
     * });
     *
     * // Stream a CSV whose header includes fields from every item in the range
     * await dataset.exportToStream(writable, {
     *   format: 'csv',
     *   collectAllKeys: true,
     * });
     * ```
     *
     * @param stream The destination Node.js `Writable`. It is neither ended nor destroyed by this method.
     * @param [options] Export options: `offset`, `limit`, `desc`, `collectAllKeys`, `format`/`contentType`,
     *   `pageSize` and an optional `AbortSignal`.
     * @param [contentType] Allows `application/json` (default) or `text/csv`; overrides the option.
     */
    async exportToStream(
        stream: Writable,
        options: DatasetExportToStreamOptions = {},
        contentType?: string,
    ): Promise<void> {
        rejectOperationInTransaction(
            'Dataset.exportToStream()',
            'the caller owns the destination stream and writes cannot be rolled back.',
        );

        const format = resolveExportFormat(options, contentType);
        const pageSize = options.pageSize ?? DATASET_ITERATORS_DEFAULT_LIMIT;

        if (!Number.isInteger(pageSize) || pageSize <= 0) {
            throw new Error('Dataset.exportToStream(): The "pageSize" option must be a positive integer.');
        }

        const offset = options.offset ?? 0;
        const limit = options.limit;

        if (!Number.isInteger(offset) || offset < 0) {
            throw new Error('Dataset.exportToStream(): The "offset" option must be a non-negative integer.');
        }

        if (limit !== undefined && (!Number.isInteger(limit) || limit < 0)) {
            throw new Error('Dataset.exportToStream(): The "limit" option must be a non-negative integer.');
        }

        const signal = options.signal;
        assertOpenWritable(stream);

        if (signal?.aborted) throw createAbortError(signal);

        let failure: Error | undefined;
        let rejectPendingWrite: ((error: Error) => void) | undefined;

        const setFailure = (error: Error) => {
            if (failure) return;
            failure = error;
            const reject = rejectPendingWrite;
            rejectPendingWrite = undefined;
            reject?.(error);
        };

        const throwIfStopped = () => {
            if (failure) throw failure;
            if (signal?.aborted) throw createAbortError(signal);
            if (stream.destroyed || stream.writableEnded) throw createStreamClosedError();
        };

        const onError = (error: Error) => setFailure(error);
        const onClose = () => setFailure(createStreamClosedError());
        const onFinish = () => setFailure(createStreamClosedError());
        const onAbort = () => setFailure(createAbortError(signal!));

        const write = async (chunk: string): Promise<void> => {
            throwIfStopped();

            await new Promise<void>((resolve, reject) => {
                let settled = false;
                let drained = false;
                let written = false;

                const finish = (error?: Error) => {
                    if (settled) return;
                    settled = true;
                    stream.off('drain', onDrain);
                    rejectPendingWrite = undefined;

                    if (error) reject(error);
                    else resolve();
                };

                const maybeFinish = () => {
                    if (drained && written) finish();
                };

                const onDrain = () => {
                    drained = true;
                    stream.off('drain', onDrain);
                    maybeFinish();
                };

                rejectPendingWrite = finish;

                let accepted: boolean;
                try {
                    // Waiting for the write callback also surfaces failures of chunks that were
                    // accepted into the buffer but could not be flushed.
                    accepted = stream.write(chunk, (error) => {
                        written = true;
                        if (error) finish(error);
                        else maybeFinish();
                    });
                } catch (error) {
                    finish(error as Error);
                    return;
                }

                // The write callback may already have settled us (for example when it failed
                // synchronously); in that case no drain listener may be registered.
                if (settled) return;

                if (accepted) {
                    // The callback surfaces failures of a chunk that was accepted into the buffer
                    // but could not be flushed; resolution waits for it.
                    drained = true;
                    maybeFinish();
                } else {
                    // The stream asked us to wait. No further backend read may start until it drains;
                    // the callback is not required here, only `drain` (or a failure) advances us.
                    written = true;
                    stream.on('drain', onDrain);
                }

                // A failure can land between `write()` and the listener registration.
                if (failure) finish(failure);
            });

            throwIfStopped();
        };

        stream.on('error', onError);
        stream.on('close', onClose);
        stream.on('finish', onFinish);
        signal?.addEventListener('abort', onAbort);

        try {
            const total = (await this.backend.getMetadata()).itemCount;
            throwIfStopped();

            const { start, end, desc } = resolveExportRange(total, offset, limit, options.desc ?? false);
            const createPages = () => this.#streamRange(start, end, desc, pageSize, throwIfStopped);

            if (format === 'json') {
                let opened = false;

                for await (const items of createPages()) {
                    for (const item of items) {
                        if (!opened) {
                            await write('[\n');
                            opened = true;
                        } else {
                            await write(',\n');
                        }

                        const serialized = jsonStringifyExtended(item, null, 2);
                        await write(`  ${serialized.replaceAll('\n', '\n  ')}`);
                    }
                }

                // Matches what an empty `exportToJSON()` stores.
                await write(opened ? '\n]' : '[]');
                return;
            }

            const { stringify } = await import('csv-stringify/sync');
            throwIfStopped();

            let keys: string[] | undefined;

            if (options.collectAllKeys) {
                // Only the column-name set is retained; items are re-read from the same frozen range.
                const collected = new Set<string>();

                for await (const items of createPages()) {
                    for (const item of items) {
                        for (const key of Object.keys(item)) collected.add(key);
                    }
                }

                keys = [...collected];
            }

            let headerWritten = false;

            for await (const items of createPages()) {
                if (items.length === 0) continue;

                // The header and every row below come from the same frozen range.
                keys ??= Object.keys(items[0]);

                if (!headerWritten) {
                    await write(stringify([keys]));
                    headerWritten = true;
                }

                for (const item of items) {
                    await write(stringify([keys.map((key) => item[key])]));
                }
            }
        } finally {
            stream.off('error', onError);
            stream.off('close', onClose);
            stream.off('finish', onFinish);
            signal?.removeEventListener('abort', onAbort);
            rejectPendingWrite = undefined;
        }
    }

    /**
     * Returns an object containing general information about the dataset.
     *
     * **Example:**
     * ```
     * {
     *   id: "WkzbQMuFYuamGv3YF",
     *   name: "my-dataset",
     *   createdAt: new Date("2015-12-12T07:34:14.202Z"),
     *   modifiedAt: new Date("2015-12-13T08:36:13.202Z"),
     *   accessedAt: new Date("2015-12-14T08:36:13.202Z"),
     *   itemCount: 14,
     * }
     * ```
     *
     * @throws If the underlying storage no longer exists (e.g. it was deleted externally).
     */
    async getInfo(): Promise<DatasetInfo> {
        const buffered = this.#bufferedJournalEntries();
        const metadata = await this.backend.getMetadata();

        if (buffered?.length) {
            const lastWriteAt = buffered[buffered.length - 1].recordedAt;
            return {
                ...metadata,
                itemCount: metadata.itemCount + buffered.reduce((sum, entry) => sum + entry.items.length, 0),
                modifiedAt: metadata.modifiedAt > lastWriteAt ? metadata.modifiedAt : lastWriteAt,
            };
        }

        return metadata;
    }

    /**
     * Iterates over dataset items, yielding each in turn to an `iteratee` function.
     * Each invocation of `iteratee` is called with two arguments: `(item, index)`.
     *
     * If the `iteratee` function returns a Promise then it is awaited before the next call.
     * If it throws an error, the iteration is aborted and the `forEach` function throws the error.
     *
     * **Example usage**
     * ```javascript
     * const dataset = await Dataset.open('my-results');
     * await dataset.forEach(async (item, index) => {
     *   console.log(`Item at ${index}: ${JSON.stringify(item)}`);
     * });
     * ```
     *
     * @param iteratee A function that is called for every item in the dataset.
     * @param [options] All `forEach()` parameters.
     * @param [index] Specifies the initial index number passed to the `iteratee` function.
     * @default 0
     */
    async forEach(iteratee: DatasetConsumer<Data>, options: DatasetIteratorOptions = {}, index = 0): Promise<void> {
        tryCancel();

        if (!options.offset) options.offset = 0;
        if (options.format && options.format !== 'json')
            throw new Error('Dataset.forEach/map/reduce() support only a "json" format.');
        if (!options.limit) options.limit = DATASET_ITERATORS_DEFAULT_LIMIT;

        const { items, total, limit, offset } = await this.getData(options);

        for (const item of items) {
            await iteratee(item, index++);
        }

        const newOffset = offset + limit;
        if (newOffset >= total) return;

        const newOpts = { ...options, offset: newOffset };
        await this.forEach(iteratee, newOpts, index);
    }

    /**
     * Produces a new array of values by mapping each value in list through a transformation function `iteratee()`.
     * Each invocation of `iteratee()` is called with two arguments: `(element, index)`.
     *
     * If `iteratee` returns a `Promise` then it's awaited before a next call.
     *
     * @param iteratee
     * @param [options] All `map()` parameters.
     */
    async map<R>(iteratee: DatasetMapper<Data, R>, options: DatasetIteratorOptions = {}): Promise<R[]> {
        tryCancel();

        const result: R[] = [];

        await this.forEach(async (item, index) => {
            const res = await iteratee(item, index);
            result.push(res);
        }, options);

        return result;
    }

    /**
     * Reduces a list of values down to a single value.
     *
     * The first element of the dataset is the initial value, with each successive reductions should
     * be returned by `iteratee()`. The `iteratee()` is passed three arguments: the `memo`, `value`
     * and `index` of the current element being folded into the reduction.
     *
     * The `iteratee` is first invoked on the second element of the list (`index = 1`), with the
     * first element given as the memo parameter. After that, the rest of the elements in the
     * dataset is passed to `iteratee`, with the result of the previous invocation as the memo.
     *
     * If `iteratee()` returns a `Promise` it's awaited before a next call.
     *
     * If the dataset is empty, reduce will return undefined.
     *
     * @param iteratee
     */
    async reduce(iteratee: DatasetReducer<Data, Data>): Promise<Data | undefined>;

    /**
     * Reduces a list of values down to a single value.
     *
     * The first element of the dataset is the initial value, with each successive reductions should
     * be returned by `iteratee()`. The `iteratee()` is passed three arguments: the `memo`, `value`
     * and `index` of the current element being folded into the reduction.
     *
     * The `iteratee` is first invoked on the second element of the list (`index = 1`), with the
     * first element given as the memo parameter. After that, the rest of the elements in the
     * dataset is passed to `iteratee`, with the result of the previous invocation as the memo.
     *
     * If `iteratee()` returns a `Promise` it's awaited before a next call.
     *
     * If the dataset is empty, reduce will return undefined.
     *
     * @param iteratee
     * @param memo Unset parameter, necessary to be able to pass options
     * @param [options] An object containing extra options for `reduce()`
     */
    async reduce(
        iteratee: DatasetReducer<Data, Data>,
        memo: undefined,
        options: DatasetIteratorOptions,
    ): Promise<Data | undefined>;

    /**
     * Reduces a list of values down to a single value.
     *
     * Memo is the initial state of the reduction, and each successive step of it should be returned
     * by `iteratee()`. The `iteratee()` is passed three arguments: the `memo`, then the `value` and
     * `index` of the iteration.
     *
     * If `iteratee()` returns a `Promise` then it's awaited before a next call.
     *
     * @param iteratee
     * @param memo Initial state of the reduction.
     * @param [options] An object containing extra options for `reduce()`
     */
    async reduce<T>(iteratee: DatasetReducer<T, Data>, memo: T, options?: DatasetIteratorOptions): Promise<T>;

    async reduce<T = Data>(
        iteratee: DatasetReducer<T, Data>,
        memo?: T,
        options: DatasetIteratorOptions = {},
    ): Promise<T | undefined> {
        tryCancel();

        let currentMemo: T | undefined = memo;

        const wrappedFunc: DatasetConsumer<Data> = async (item, index) => {
            if (index === 0 && currentMemo === undefined) {
                currentMemo = item;
            } else {
                // We are guaranteed that currentMemo is instanciated, since we are either not on
                // the first iteration, or memo was already set by the user.
                currentMemo = await iteratee(currentMemo as T, item, index);
            }
        };

        await this.forEach(wrappedFunc, options);
        return currentMemo;
    }

    async *#fetchEntryPages(options: DatasetIteratorOptions): AsyncGenerator<PaginatedList<[number, Data]>> {
        let index = options.offset ?? 0;
        for await (const page of this.#fetchPages(options)) {
            yield {
                ...page,
                items: page.items.map((item) => [index++, item] as [number, Data]),
            };
        }
    }

    async *#fetchPages(
        options: DatasetIteratorOptions,
        pageSize = DATASET_ITERATORS_DEFAULT_LIMIT,
    ): AsyncGenerator<PaginatedList<Data>> {
        let offset = options.offset ?? 0;
        const totalLimit = options.limit;
        let yielded = 0;

        while (true) {
            const fetchLimit = totalLimit !== undefined ? Math.min(pageSize, totalLimit - yielded) : pageSize;
            if (fetchLimit <= 0) break;

            const page = await this.#readPage({ ...options, offset, limit: fetchLimit });
            yield page;

            yielded += page.items.length;
            if (page.items.length < fetchLimit || offset + page.items.length >= page.total) break;
            offset += page.items.length;
        }
    }

    /**
     * Reads the frozen `[start, end)` index range in the requested output order, one bounded page at
     * a time. Descending output is served by reading ascending chunks from the end and reversing
     * each chunk, so concurrent appends (which only grow the dataset past `end`) can never shift the
     * window or produce duplicates.
     */
    async *#streamRange(
        start: number,
        end: number,
        desc: boolean,
        pageSize: number,
        check: () => void,
    ): AsyncGenerator<Data[]> {
        if (!desc) {
            let cursor = start;

            while (cursor < end) {
                check();

                const limit = Math.min(pageSize, end - cursor);
                this.#statsTracker.add('readCount');
                const page = await this.backend.getData({ offset: cursor, limit, desc: false });
                check();

                const items = page.items.slice(0, limit);
                cursor += items.length;

                if (items.length === 0) return;

                yield items;

                if (items.length < limit) return;
            }

            return;
        }

        let cursor = end;

        while (cursor > start) {
            check();

            const limit = Math.min(pageSize, cursor - start);
            const offset = cursor - limit;
            this.#statsTracker.add('readCount');
            const page = await this.backend.getData({ offset, limit, desc: false });
            check();

            const items = page.items.slice(0, limit);
            cursor = offset;

            if (items.length === 0) return;

            yield items.reverse();

            if (items.length < limit) return;
        }
    }

    /**
     * Returns dataset items.
     *
     * When awaited (`await dataset.values()`), returns all items as a flat `Data[]` array.
     * When used as an async iterable (`for await...of`), iterates over all items across pages
     * without loading everything into memory at once.
     *
     * **Example usage:**
     * ```javascript
     * const dataset = await Dataset.open('my-results');
     *
     * // Iterate over all items (memory-efficient for large datasets)
     * for await (const item of dataset.values()) {
     *   console.log(item);
     * }
     *
     * // Or fetch all items at once
     * const items = await dataset.values();
     * console.log(items);
     * ```
     *
     * @param options Options for the iteration.
     */
    values(options: DatasetIteratorOptions = {}): AsyncIterable<Data> & Promise<Data[]> {
        tryCancel();

        return createDualIterable({
            createPages: () => this.#fetchPages(options),
            extractItems: (page) => page.items,
        });
    }

    /**
     * Returns dataset entries (index-value pairs).
     *
     * When awaited (`await dataset.entries()`), returns all entries as a flat `[index, item][]` array.
     * When used as an async iterable (`for await...of`), iterates over all entries across pages
     * without loading everything into memory at once.
     *
     * **Example usage:**
     * ```javascript
     * const dataset = await Dataset.open('my-results');
     *
     * // Iterate over all entries
     * for await (const [index, item] of dataset.entries()) {
     *   console.log(`Item at ${index}: ${JSON.stringify(item)}`);
     * }
     *
     * // Or fetch all at once
     * const entries = await dataset.entries();
     * console.log(entries);
     * ```
     *
     * @param options Options for the iteration.
     */
    entries(options: DatasetIteratorOptions = {}): AsyncIterable<[number, Data]> & Promise<[number, Data][]> {
        tryCancel();

        return createDualIterable({
            createPages: () => this.#fetchEntryPages(options),
            extractItems: (page) => page.items,
        });
    }

    /**
     * Default async iterator for the dataset, iterating over items.
     * Allows using the dataset directly in a `for await...of` loop.
     *
     * **Example usage:**
     * ```javascript
     * const dataset = await Dataset.open('my-results');
     * for await (const item of dataset) {
     *   console.log(item);
     * }
     * ```
     */
    async *[Symbol.asyncIterator](): AsyncGenerator<Data, void, undefined> {
        yield* this.values();
    }

    /**
     * Removes the dataset either from the Apify cloud storage or from the local directory,
     * depending on the mode of operation.
     */
    async drop(): Promise<void> {
        rejectOperationInTransaction('Dataset.drop()');

        await this.backend.drop();
        serviceLocator.getStorageInstanceManager().removeFromCache(this);
    }

    /**
     * Removes all items from the dataset but keeps the dataset itself, along with its
     * {@apilink Dataset.id|`id`} and {@apilink Dataset.name|`name`}.
     */
    async purge(): Promise<void> {
        rejectOperationInTransaction('Dataset.purge()');

        await this.backend.purge();
    }

    /**
     * Opens a dataset and returns a promise resolving to an instance of the {@apilink Dataset} class.
     *
     * Datasets are used to store structured data where each object stored has the same attributes,
     * such as online store products or real estate offers.
     * The actual data is stored either on the local filesystem or in the cloud.
     *
     * For more details and code examples, see the {@apilink Dataset} class.
     *
     * @param [identifier]
     *   ID or name of the dataset to be opened. If a string is provided, it will first be
     *   looked up as an ID; if no such storage exists, it will be treated as a name.
     *   If `null` or `undefined`, the function returns the default dataset associated with the crawler run.
     * @param [options] Storage manager options.
     */
    static async open<Data extends Dictionary = Dictionary>(
        identifier?: string | StorageIdentifier | null,
        options: StorageOpenOptions = {},
    ): Promise<Dataset<Data>> {
        tryCancel();

        const parsedOptions = parseArgument(options, openOptionsSchema);

        const configuration = parsedOptions.configuration ?? Configuration.getGlobalConfiguration();
        const storageBackend = parsedOptions.storageBackend ?? serviceLocator.getStorageBackend();

        await purgeDefaultStorages({ onlyPurgeOnce: true, storageBackend, configuration });

        const resolved = await resolveStorageIdentifier(identifier, storageBackend, 'Dataset');

        return serviceLocator.getStorageInstanceManager().openStorage<Dataset<Data>>(this, {
            ...resolved,
            backendOpener: () => storageBackend.createDatasetBackend(resolved),
            backendCacheKey: storageBackend.getStorageBackendCacheKey?.() ?? storageBackend.constructor.name,
        });
    }

    /**
     * Stores an object or an array of objects to the default {@apilink Dataset} of the current crawler run.
     *
     * This is just a convenient shortcut for {@apilink Dataset.pushData}.
     * For example, calling the following code:
     * ```javascript
     * await Dataset.pushData({ myValue: 123 });
     * ```
     *
     * is equivalent to:
     * ```javascript
     * const dataset = await Dataset.open();
     * await dataset.pushData({ myValue: 123 });
     * ```
     *
     * For more information, see {@apilink Dataset.open} and {@apilink Dataset.pushData}
     *
     * **IMPORTANT**: Make sure to use the `await` keyword when calling `pushData()`,
     * otherwise the crawler process might finish before the data are stored!
     *
     * @param item Object or array of objects containing data to be stored in the default dataset.
     * The objects must be serializable to JSON and the JSON representation of each object must be smaller than 9MB.
     * @ignore
     */
    static async pushData<Data extends Dictionary = Dictionary>(item: Data | Data[]): Promise<void> {
        const dataset = await this.open();
        return dataset.pushData(item);
    }

    /**
     * Returns {@apilink DatasetContent} object holding the items in the dataset based on the provided parameters.
     */
    static async getData<Data extends Dictionary = Dictionary>(
        options: DatasetDataOptions = {},
    ): Promise<DatasetContent<Data>> {
        const dataset = await this.open();
        return dataset.getData(options);
    }
}

/**
 * User-function used in the `Dataset.forEach()` API.
 */
export interface DatasetConsumer<Data> {
    /**
     * @param item Current {@apilink Dataset} entry being processed.
     * @param index Position of current {@apilink Dataset} entry.
     */
    (item: Data, index: number): Awaitable<void>;
}

/**
 * User-function used in the `Dataset.map()` API.
 */
export interface DatasetMapper<Data, R> {
    /**
     * User-function used in the `Dataset.map()` API.
     * @param item Current {@apilink Dataset} entry being processed.
     * @param index Position of current {@apilink Dataset} entry.
     */
    (item: Data, index: number): Awaitable<R>;
}

/**
 * User-function used in the `Dataset.reduce()` API.
 */
export interface DatasetReducer<T, Data> {
    /**
     * @param memo Previous state of the reduction.
     * @param item Current {@apilink Dataset} entry being processed.
     * @param index Position of current {@apilink Dataset} entry.
     */
    (memo: T, item: Data, index: number): Awaitable<T>;
}

/** @internal */
export interface DatasetOptions {
    /** Resolved metadata for the dataset, as returned by the backend's `getMetadata()`. */
    metadata: DatasetInfo;
    backend: DatasetBackend;
}

export interface DatasetContent<Data> {
    /** Total count of entries in the dataset. */
    total: number;
    /** Count of dataset entries returned in this set. */
    count: number;
    /** Position of the first returned entry in the dataset. */
    offset: number;
    /** Maximum number of dataset entries requested. */
    limit: number;
    /** Dataset entries based on chosen format parameter. */
    items: Data[];
    /** Should the results be in descending order. */
    desc?: boolean;
}

function resolveExportFormat(options: DatasetExportToStreamOptions, contentType?: string): 'json' | 'csv' {
    const requested =
        contentType ??
        options.contentType ??
        (options.format === 'csv' ? 'text/csv' : options.format === 'json' ? 'application/json' : undefined);

    if (requested === undefined) return 'json';

    const normalized = requested.toLowerCase().split(';', 1)[0].trim();

    if (normalized === 'application/json') return 'json';
    if (normalized === 'text/csv') return 'csv';

    throw new Error(`Dataset.exportToStream(): Unsupported content type: ${requested}`);
}

/**
 * Turns `offset`/`limit`/`desc` into the absolute ascending index range `[start, end)` that the
 * export will read. `offset` keeps the {@apilink Dataset.getData} meaning: with `desc` it counts
 * from the newest item. The range is computed from the item count captured before the first read,
 * so later appends cannot enter it.
 */
function resolveExportRange(
    total: number,
    offset: number,
    limit: number | undefined,
    desc: boolean,
): { start: number; end: number; desc: boolean } {
    const skipped = Math.min(offset, total);

    if (!desc) {
        return {
            start: skipped,
            end: limit === undefined ? total : Math.min(total, skipped + limit),
            desc,
        };
    }

    // With `desc`, `offset` newest items are skipped and the window grows towards older indices.
    const end = Math.max(0, total - skipped);
    const start = limit === undefined ? 0 : Math.max(0, end - limit);

    return { start, end, desc };
}

function assertOpenWritable(stream: unknown): asserts stream is Writable {
    if (
        stream === null ||
        typeof stream !== 'object' ||
        typeof (stream as Writable).write !== 'function' ||
        typeof (stream as Writable).on !== 'function' ||
        typeof (stream as Writable).off !== 'function'
    ) {
        throw new Error('Dataset.exportToStream(): The "stream" argument must be a writable Node.js stream.');
    }

    const writable = stream as Writable;

    if (writable.destroyed || writable.writableEnded) {
        throw createStreamClosedError();
    }
}

function createStreamClosedError(): Error {
    return new Error(
        'Dataset.exportToStream(): The destination stream was closed or ended before the export finished.',
    );
}

function createAbortError(signal: AbortSignal): Error {
    if (signal.reason instanceof Error) return signal.reason;

    const error = new Error(
        signal.reason === undefined
            ? 'Dataset.exportToStream() was aborted.'
            : `Dataset.exportToStream() was aborted: ${String(signal.reason)}`,
    );
    error.name = 'AbortError';

    return error;
}
