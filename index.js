/**
 * Copyright JS Foundation and other contributors, http://js.foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

/**
 * Redis based context storage for Node-RED.
 *
 * Configuration options:
 * {
 *    host: '127.0.0.1',       // Redis server host.                        default: '127.0.0.1'
 *    port: 6379,              // Redis server port.                        default: 6379
 *    db: 0,                   // Redis logical database.                   default: 0
 *    prefix: 'nodered',       // Prefix for every key written by the plugin.
 *                             // Strongly recommended: clean() only removes stale
 *                             // context when a prefix is configured.      default: undefined
 *    username:                // Redis ACL user (Redis >= 6).              default: undefined
 *    password:                // Password sent with AUTH on connect.       default: undefined
 *    tls:                     // true, or an object of tls.connect options. default: undefined
 *    url:                     // Alternative to host/port, e.g. 'redis://user:pass@host:6379/0'
 *    reconnectStrategy:       // See node-redis: false | number | (retries, cause) => false|number|Error
 *                             // default: node-redis default (retry with backoff)
 *    retry_strategy:          // Legacy alias of reconnectStrategy.
 *    connectTimeout:          // Socket connect timeout in ms.             default: node-redis default
 *    clientOptions:           // Extra options merged into createClient(). default: undefined
 * }
 *
 * Key layout (see Readme.md):
 *   [prefix:]global:<key>            global context
 *   [prefix:]<flowId>:<key>          flow context
 *   [prefix:]<nodeId>:<flowId>:<key> node context
 *
 * Each root context property is stored as one Redis string holding its JSON
 * representation, exactly as JSON.stringify produces it.
 */

'use strict';

const redis = require('redis');
const safeJSONStringify = require('json-stringify-safe');
const { util, log } = loadNodeRedUtil();

const WATCH_RETRIES = 10;   // optimistic-lock retries for nested writes
const DEL_BATCH = 500;      // keys per DEL command in delete()/clean()

/**
 * Load @node-red/util from the running Node-RED instance when possible, so that
 * property-expression semantics are identical to the built-in context stores.
 */
function loadNodeRedUtil() {
    if (process.env.NODE_RED_HOME) {
        try {
            return require(require.resolve('@node-red/util', { paths: [process.env.NODE_RED_HOME] }));
        } catch (err) {
            // fall through
        }
    }
    try {
        return require('@node-red/util');
    } catch (err) {
        throw new Error('node-red-context-redis: cannot resolve @node-red/util. ' +
            'Load this plugin from a Node-RED settings.js, or install @node-red/util alongside it.');
    }
}

function stringify(value) {
    let circular = false;
    const json = safeJSONStringify(value, null, null, function () { circular = true; });
    return { json, circular };
}

function unique(list) {
    return list.filter((item, index) => list.indexOf(item) === index);
}

// Escape glob metacharacters so ids/prefixes are matched literally by SCAN.
function escapeGlob(str) {
    return String(str).replace(/[\\*?[\]]/g, '\\$&');
}

// Invoke a user callback outside of the promise chain, so an exception thrown
// by the callback surfaces as an uncaught exception rather than being swallowed
// or reported as an error of the store operation.
function invoke(callback, args) {
    try {
        callback.apply(null, args);
    } catch (err) {
        process.nextTick(() => { throw err; });
    }
}

function Redis(config) {
    config = config || {};
    this.prefix = config.prefix;
    this.host = config.host || '127.0.0.1';
    this.port = config.port || 6379;

    const socket = {};
    if (!config.url) {
        socket.host = this.host;
        socket.port = this.port;
    }
    if (config.tls) {
        socket.tls = true;
        if (typeof config.tls === 'object') {
            Object.assign(socket, config.tls);
        }
    }
    if (config.connectTimeout !== undefined) {
        socket.connectTimeout = config.connectTimeout;
    }
    const strategy = config.reconnectStrategy !== undefined ? config.reconnectStrategy : config.retry_strategy;
    if (typeof strategy === 'function') {
        // Legacy node_redis v2 strategies returned undefined to stop retrying;
        // node-redis expects false in that case.
        socket.reconnectStrategy = (retries, cause) => {
            const result = strategy(retries, cause);
            return result === undefined ? false : result;
        };
    } else if (strategy !== undefined) {
        socket.reconnectStrategy = strategy;
    }

    this.options = Object.assign({
        socket,
        database: config.db || 0,
        username: config.username,
        password: config.password
    }, config.url ? { url: config.url } : {}, config.clientOptions || {});

    this.client = null;
    this.knownCircularRefs = {};
    this._writeQueue = Promise.resolve();
    this._warnedNoPrefix = false;
}

Redis.prototype._key = function (scope, key) {
    return (this.prefix ? this.prefix + ':' : '') + scope + ':' + key;
};

Redis.prototype._pattern = function (scope) {
    return (this.prefix ? escapeGlob(this.prefix) + ':' : '') + escapeGlob(scope) + ':*';
};

// Serialise write transactions: WATCH/MULTI/EXEC state is per connection, and
// an EXEC clears every watched key, so transactions must not interleave.
Redis.prototype._enqueue = function (task) {
    const run = this._writeQueue.then(task, task);
    this._writeQueue = run.catch(() => { });
    return run;
};

Redis.prototype._scan = async function (pattern, onBatch) {
    for await (const batch of this.client.scanIterator({ MATCH: pattern, COUNT: 1000 })) {
        const keys = Array.isArray(batch) ? batch : [batch];
        if (keys.length > 0) {
            await onBatch(keys);
        }
    }
};

Redis.prototype._deleteKeys = async function (keys) {
    for (let i = 0; i < keys.length; i += DEL_BATCH) {
        await this.client.del(keys.slice(i, i + DEL_BATCH));
    }
};

Redis.prototype.open = async function () {
    this.client = redis.createClient(this.options);
    this.client.on('error', (err) => {
        log.error('[context:redis] ' + (err && err.message ? err.message : err));
    });
    await this.client.connect();
};

Redis.prototype.close = async function () {
    if (!this.client) {
        return;
    }
    const client = this.client;
    this.client = null;
    try {
        await this._writeQueue;
    } catch (err) {
        // already reported to the caller of set()
    }
    if (client.isOpen) {
        if (typeof client.close === 'function') {
            await client.close();
        } else {
            await client.quit();
        }
    }
};

Redis.prototype.get = function (scope, key, callback) {
    if (typeof callback !== 'function') {
        throw new Error('node-red-context-redis: synchronous context access is not supported. ' +
            'Use the asynchronous form, e.g. flow.get("key", "redis", (err, value) => { ... })');
    }
    let keys;
    let rootKeys;
    try {
        keys = Array.isArray(key) ? key : [key];
        rootKeys = unique(keys.map(k => util.normalisePropertyExpression(k)[0]));
    } catch (err) {
        return callback(err);
    }
    this.client.mGet(rootKeys.map(k => this._key(scope, k))).then((replies) => {
        const data = {};
        for (let i = 0; i < rootKeys.length; i++) {
            if (replies[i] !== null && replies[i] !== undefined) {
                try {
                    data[rootKeys[i]] = JSON.parse(replies[i]);
                } catch (err) {
                    // Not JSON (written by someone else?) - treat as undefined
                }
            }
        }
        return keys.map((k) => {
            try {
                return util.getObjectProperty(data, k);
            } catch (err) {
                if (err.code === 'INVALID_EXPR') {
                    throw err;
                }
                return undefined;
            }
        });
    }).then(
        (results) => invoke(callback, [null, ...results]),
        (err) => invoke(callback, [err])
    );
};

Redis.prototype.set = function (scope, key, value, callback) {
    if (callback && typeof callback !== 'function') {
        throw new Error('Callback must be a function');
    }
    let plan;
    try {
        const keys = Array.isArray(key) ? key : [key];
        let values;
        if (!Array.isArray(key)) {
            values = [value];
        } else if (!Array.isArray(value)) {
            values = [value];
        } else {
            values = value;
        }
        plan = keys.map((k, i) => ({
            key: k,
            value: i < values.length ? values[i] : null,
            parts: util.normalisePropertyExpression(k)
        }));
    } catch (err) {
        if (callback) {
            return callback(err);
        }
        throw err;
    }

    const rootKeys = unique(plan.map(p => p.parts[0]));
    const redisKeys = rootKeys.map(k => this._key(scope, k));
    const needsRead = plan.some(p => p.parts.length > 1);

    const write = async () => {
        for (let attempt = 0; ; attempt++) {
            const data = {};
            let watching = false;
            try {
                if (needsRead) {
                    // Nested property: read-modify-write under optimistic lock.
                    await this.client.watch(redisKeys);
                    watching = true;
                    const replies = await this.client.mGet(redisKeys);
                    for (let i = 0; i < rootKeys.length; i++) {
                        if (replies[i] !== null && replies[i] !== undefined) {
                            try {
                                data[rootKeys[i]] = JSON.parse(replies[i]);
                            } catch (err) {
                                // Corrupt/non-JSON value: it will be overwritten
                            }
                        }
                    }
                }
                for (const p of plan) {
                    util.setObjectProperty(data, p.key, p.value);
                }
                const mset = [];
                const del = [];
                for (let i = 0; i < rootKeys.length; i++) {
                    const rootKey = rootKeys[i];
                    const redisKey = redisKeys[i];
                    if (data[rootKey] === undefined) {
                        delete this.knownCircularRefs[redisKey];
                        del.push(redisKey);
                    } else {
                        const result = stringify(data[rootKey]);
                        if (result.circular && !this.knownCircularRefs[redisKey]) {
                            log.warn(log._('context.localfilesystem.error-circular', { scope: redisKey }));
                            this.knownCircularRefs[redisKey] = true;
                        } else if (!result.circular) {
                            delete this.knownCircularRefs[redisKey];
                        }
                        mset.push(redisKey, result.json);
                    }
                }
                const multi = this.client.multi();
                if (mset.length > 0) {
                    multi.mSet(mset);
                }
                if (del.length > 0) {
                    multi.del(del);
                }
                watching = false; // EXEC (success or WatchError) always clears the watch
                await multi.exec();
                return;
            } catch (err) {
                if (watching) {
                    await this.client.unwatch().catch(() => { });
                }
                if (err instanceof redis.WatchError && attempt < WATCH_RETRIES) {
                    continue;
                }
                throw err;
            }
        }
    };

    this._enqueue(write).then(
        () => { if (callback) { invoke(callback, [null]); } },
        (err) => { if (callback) { invoke(callback, [err]); } }
    );
};

Redis.prototype.keys = function (scope, callback) {
    if (typeof callback !== 'function') {
        throw new Error('Callback must be a function');
    }
    const strip = (this.prefix ? this.prefix + ':' : '').length + (scope + ':').length;
    const result = [];
    this._scan(this._pattern(scope), (keys) => {
        keys.forEach(k => result.push(k.substring(strip)));
    }).then(
        () => invoke(callback, [null, result]),
        (err) => invoke(callback, [err])
    );
};

Redis.prototype.delete = function (scope) {
    return this._scan(this._pattern(scope), keys => this._deleteKeys(keys));
};

/**
 * Remove the context of nodes/flows that no longer exist.
 * `activeNodes` holds the ids of all nodes and flows in the current deployment;
 * a key belongs to an active scope when its first id segment is one of them.
 */
Redis.prototype.clean = async function (activeNodes) {
    this.knownCircularRefs = {};
    if (!this.prefix) {
        if (!this._warnedNoPrefix) {
            this._warnedNoPrefix = true;
            log.warn('[context:redis] no "prefix" configured - stale context is not cleaned up, ' +
                'because unrelated keys in the same Redis database cannot be told apart from context keys');
        }
        return;
    }
    const active = new Set(activeNodes || []);
    const prefixLen = (this.prefix + ':').length;
    await this._scan(escapeGlob(this.prefix) + ':*', async (keys) => {
        const stale = keys.filter((key) => {
            const scopeId = key.substring(prefixLen).split(':')[0];
            return scopeId !== 'global' && !active.has(scopeId);
        });
        await this._deleteKeys(stale);
    });
};

module.exports = function (config) {
    return new Redis(config);
};
