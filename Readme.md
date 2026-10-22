# node-red-context-redis

A [Node-RED context store](https://nodered.org/docs/api/context/) backed by Redis.

Tested with Node.js 18/20/22, Redis 7 and Node-RED 5.

## Install

In your Node-RED user directory (typically `~/.node-red`, or `/data` in the official Docker image):

```sh
npm install git+https://github.com/node-red/node-red-context-redis
```

Then register the store in `settings.js`:

```javascript
contextStorage: {
    default: "memoryOnly",
    memoryOnly: { module: "memory" },
    redis: {
        module: require("node-red-context-redis"),
        config: {
            host: process.env.REDIS_HOST || "127.0.0.1",
            port: 6379,
            password: process.env.REDIS_PASSWORD,
            prefix: "nodered"
        }
    }
}
```

The module must be passed with `require(...)`: Node-RED treats a plain string as the name of a built-in store.

## Options

| Option              | Description                                                                                                   |
| ------------------- | ------------------------------------------------------------------------------------------------------------- |
| `host`              | Redis host. Default `"127.0.0.1"`                                                                             |
| `port`              | Redis port. Default `6379`                                                                                    |
| `db`                | Redis logical database. Default `0`                                                                           |
| `prefix`            | Prefix for every key written by the plugin. **Recommended**: without a prefix the plugin cannot tell its keys from anything else in the database, so stale context is never cleaned up. Use a different prefix per Node-RED instance sharing the same Redis. |
| `username`          | Redis ACL user (Redis 6+)                                                                                     |
| `password`          | Password sent with `AUTH` on connect                                                                          |
| `tls`               | `true`, or an object of [`tls.connect`](https://nodejs.org/api/tls.html#tlsconnectoptions-callback) options    |
| `url`               | `redis://user:pass@host:port/db` – alternative to `host`/`port`/`db`                                           |
| `reconnectStrategy` | `false`, a delay in ms, or `(retries, cause) => false \| ms \| Error`. See the [node-redis docs](https://github.com/redis/node-redis/blob/master/docs/client-configuration.md). Default: retry with backoff. `retry_strategy` is accepted as a legacy alias. |
| `connectTimeout`    | Socket connect timeout in ms                                                                                   |
| `clientOptions`     | Extra options merged into node-redis `createClient()`                                                         |

With the default reconnect strategy `open()` waits until Redis is reachable, so Node-RED startup blocks until the connection succeeds.

## Usage notes

- Access is asynchronous only. Use the callback form in Function nodes:

  ```javascript
  flow.get("counter", "redis", (err, value) => { ... });
  flow.set("counter", 1, "redis", (err) => { ... });
  ```

  `flow.get("counter", "redis")` without a callback throws, as it must for any network-backed store.

- Every write is a Redis transaction. Writes to a nested property (`flow.set("obj.a.b", 1)`) read the root value, apply the change with Node-RED's own property logic and write it back under `WATCH`, so concurrent writers (including other processes) cannot lose updates. Writes are serialised per connection.

- On deploy, Node-RED asks the store to drop the context of nodes and flows that no longer exist. This only happens when a `prefix` is configured (see above).

## Data model

Each root context property is one Redis string holding its JSON value, exactly as produced by `JSON.stringify`:

```text
Node-RED                                  Redis key
global.set("foo", "bar")               -> [prefix:]global:foo                  "\"bar\""
flow.set("obj", {a: 1})                -> [prefix:]<flowId>:obj                "{\"a\":1}"
context.set("n", 1)   (in a node)      -> [prefix:]<nodeId>:<flowId>:n         "1"
```

Values can be read by any other client:

```console
redis> GET nodered:global:obj
"{\"key\":\"value\"}"
```

## Test

Requires a Redis server on `127.0.0.1:6379` without authentication.

```sh
npm install
npm test
```

## Changes in 0.1.0

- Nested writes no longer go through a Lua/cjson script. The old script re-encoded the whole root value with `cjson`, which truncated numbers to 14 significant digits (e.g. `9007199254740991` became `9007199254741000`) and turned empty arrays into `{}`. Values now round-trip exactly.
- `clean()` deleted nothing when no `prefix` was set (it built `undefined:...` keys). It now cleans with a prefix, matches active ids exactly, uses `SCAN` instead of `KEYS`, and leaves an unprefixed database alone.
- Client upgraded from `redis@2.8` (2019) to `redis@6`: ACL `username`, `url`, TLS and reconnect options follow node-redis.
- `@node-red/util` is resolved from the running Node-RED instance (as before) and is an optional peer dependency instead of a pinned `0.20.0-beta.2` install.
- Requires Node.js 18+.
