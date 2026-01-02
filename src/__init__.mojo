"""
mojo-redis

Pure Mojo Redis client with RESP protocol support.

A full-featured Redis client implementing the RESP (Redis Serialization Protocol)
for communication with Redis servers. Uses direct C FFI for TCP sockets.

Features:
    - RESP2 protocol encoding/decoding
    - String commands (GET, SET, INCR, DECR, etc.)
    - Hash commands (HGET, HSET, HGETALL, etc.)
    - List commands (LPUSH, RPUSH, LPOP, RPOP, LRANGE, etc.)
    - Key commands (EXISTS, DEL, EXPIRE, TTL, etc.)
    - Command pipelining for batch operations
    - Transaction support (MULTI/EXEC)
    - Connection management

Architecture:
    ┌──────────────────────────────────────────────────────────┐
    │                 mojo-redis (Pure Mojo)                    │
    ├──────────────────────────────────────────────────────────┤
    │                                                          │
    │  ┌─────────────┐  ┌──────────────┐  ┌────────────────┐  │
    │  │ RedisClient │  │   Pipeline   │  │  Transaction   │  │
    │  │  (sync)     │  │  (batching)  │  │  (MULTI/EXEC)  │  │
    │  └─────────────┘  └──────────────┘  └────────────────┘  │
    │         │                │                  │            │
    │         ▼                ▼                  ▼            │
    │  ┌──────────────────────────────────────────────────┐   │
    │  │              RESP Encoder/Decoder                 │   │
    │  │   encode_command() / decode_response()            │   │
    │  └──────────────────────────────────────────────────┘   │
    │                         │                               │
    │                         ▼                               │
    │  ┌──────────────────────────────────────────────────┐   │
    │  │           TCP Socket (C FFI)                      │   │
    │  │   socket(), connect(), send(), recv(), close()    │   │
    │  └──────────────────────────────────────────────────┘   │
    │                         │                               │
    └─────────────────────────┼───────────────────────────────┘
                              ▼
    ┌──────────────────────────────────────────────────────────┐
    │                    Redis Server                           │
    │              (localhost:6379 or remote)                   │
    └──────────────────────────────────────────────────────────┘

Usage (Basic Client):
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
        var count = client.get("counter")
        print(count.as_int())  # 11

        client.close()

Usage (Hash Operations):
    from mojo_redis import RedisClient

    fn main() raises:
        var client = RedisClient("localhost", 6379)
        client.connect()

        # Store user data
        client.hset("user:1000", "name", "Alice")
        client.hset("user:1000", "email", "alice@example.com")

        # Retrieve
        var name = client.hget("user:1000", "name")
        print(name.as_string())  # "Alice"

        # Get all fields
        var user = client.hgetall("user:1000")
        # Returns array: ["name", "Alice", "email", "alice@example.com"]

        client.close()

Usage (List Operations):
    from mojo_redis import RedisClient

    fn main() raises:
        var client = RedisClient("localhost", 6379)
        client.connect()

        # Build a queue
        client.rpush("tasks", "task1")
        client.rpush("tasks", "task2")
        client.rpush("tasks", "task3")

        # Process queue (FIFO)
        var task = client.lpop("tasks")
        print(task.as_string())  # "task1"

        # Get remaining
        var remaining = client.lrange("tasks", 0, -1)
        # Returns array: ["task2", "task3"]

        client.close()

Usage (Pipelining):
    from mojo_redis import RedisClient, Pipeline

    fn main() raises:
        var client = RedisClient("localhost", 6379)
        client.connect()

        # Pipeline for bulk operations
        var pipeline = Pipeline(&client)
        pipeline.set("key1", "value1")
        pipeline.set("key2", "value2")
        pipeline.get("key1")
        pipeline.get("key2")
        pipeline.incr("counter")

        var results = pipeline.execute()
        # 5 responses in one round trip!

        client.close()

Usage (Transactions):
    from mojo_redis import RedisClient, Transaction

    fn main() raises:
        var client = RedisClient("localhost", 6379)
        client.connect()

        # Atomic transaction
        var tx = Transaction(&client)
        tx.set("balance", "100")
        tx.incr("balance")
        tx.decr("balance")

        var results = tx.execute()
        # All commands executed atomically

        client.close()
"""

# RESP Protocol
from .resp import (
    # Types
    RespType,
    RespValue,
    # Encoder/Decoder
    RespEncoder,
    RespDecoder,
    # Utility functions
    encode_command,
    decode_response,
    # Constants
    CRLF,
    SIMPLE_STRING_PREFIX,
    ERROR_PREFIX,
    INTEGER_PREFIX,
    BULK_STRING_PREFIX,
    ARRAY_PREFIX,
)

# Client
from .client import (
    RedisConfig,
    RedisClient,
)

# Pipelining
from .pipeline import (
    Pipeline,
    PipelineCommand,
    Transaction,
)
