# mojo-redis

Pure Mojo Redis client with RESP protocol support.

## Features

- **RESP2 Protocol** - Full encoding/decoding
- **String Commands** - GET, SET, INCR, DECR, etc.
- **Hash Commands** - HGET, HSET, HGETALL, etc.
- **List Commands** - LPUSH, RPUSH, LPOP, RPOP, LRANGE
- **Pipelining** - Batch operations for performance
- **Transactions** - MULTI/EXEC support

## Installation

```bash
pixi add mojo-redis
```

## Quick Start

### Basic Operations

```mojo
from mojo_redis import RedisClient

fn main() raises:
    var client = RedisClient("localhost", 6379)
    client.connect()
    
    # String operations
    client.set("greeting", "Hello, Mojo!")
    var value = client.get("greeting")
    print(value.as_string())  # "Hello, Mojo!"
    
    # Counter operations
    client.set("counter", "0")
    client.incr("counter")
    client.incrby("counter", 10)
    
    client.close()
```

### Hash Operations

```mojo
from mojo_redis import RedisClient

fn main() raises:
    var client = RedisClient("localhost", 6379)
    client.connect()
    
    # Store user data
    client.hset("user:1000", "name", "Alice")
    client.hset("user:1000", "email", "alice@example.com")
    
    var name = client.hget("user:1000", "name")
    print(name.as_string())  # "Alice"
    
    client.close()
```

### Pipelining

```mojo
from mojo_redis import RedisClient, Pipeline

fn main() raises:
    var client = RedisClient("localhost", 6379)
    client.connect()
    
    var pipeline = Pipeline(&client)
    pipeline.set("key1", "value1")
    pipeline.set("key2", "value2")
    pipeline.get("key1")
    
    var results = pipeline.execute()  # One round trip!
    
    client.close()
```

### Transactions

```mojo
from mojo_redis import RedisClient, Transaction

fn main() raises:
    var client = RedisClient("localhost", 6379)
    client.connect()
    
    var tx = Transaction(&client)
    tx.set("balance", "100")
    tx.incr("balance")
    
    var results = tx.execute()  # Atomic execution
    
    client.close()
```

## API Reference

| Command | Method |
|---------|--------|
| GET | `get(key)` |
| SET | `set(key, value)` |
| INCR | `incr(key)` |
| HGET | `hget(key, field)` |
| HSET | `hset(key, field, value)` |
| LPUSH | `lpush(key, value)` |
| RPOP | `rpop(key)` |
| DEL | `del(key)` |
| EXISTS | `exists(key)` |

## Testing

```bash
mojo run tests/test_redis.mojo
```

## License

Apache 2.0

## Part of mojo-contrib

This library is part of [mojo-contrib](https://github.com/atsentia/mojo-contrib), a collection of pure Mojo libraries.
