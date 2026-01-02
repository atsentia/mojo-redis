"""
Example: Redis Client with RESP Protocol

Demonstrates:
- Connecting to Redis
- String operations (GET, SET, INCR)
- Hash operations (HSET, HGET)
- List operations (LPUSH, RPOP)
- Pipelining for bulk operations
- Transactions (MULTI/EXEC)
"""

from mojo_redis import RedisClient, Pipeline, Transaction


fn basic_operations() raises:
    """Basic Redis string operations."""
    print("=== Basic Operations ===")

    var client = RedisClient("localhost", 6379)
    client.connect()

    # SET and GET
    client.set("greeting", "Hello, Mojo!")
    var value = client.get("greeting")
    print("GET greeting: " + value.as_string())

    # Counter operations
    client.set("counter", "0")
    client.incr("counter")
    client.incrby("counter", 10)
    var count = client.get("counter")
    print("Counter after INCR + INCRBY(10): " + String(count.as_int()))

    # Key expiration
    client.setex("temp_key", "temporary", 60)  # 60 second TTL
    var ttl = client.ttl("temp_key")
    print("TTL of temp_key: " + String(ttl) + " seconds")

    client.close()
    print("")


fn hash_operations() raises:
    """Redis hash operations for structured data."""
    print("=== Hash Operations ===")

    var client = RedisClient("localhost", 6379)
    client.connect()

    # Store user as hash
    client.hset("user:1000", "name", "Alice")
    client.hset("user:1000", "email", "alice@example.com")
    client.hset("user:1000", "role", "admin")

    # Retrieve single field
    var name = client.hget("user:1000", "name")
    print("User name: " + name.as_string())

    # Get all fields
    var user = client.hgetall("user:1000")
    print("All fields: " + user.as_string())

    # Check field exists
    var exists = client.hexists("user:1000", "email")
    print("Has email: " + String(exists))

    client.close()
    print("")


fn list_operations() raises:
    """Redis list operations for queues/stacks."""
    print("=== List Operations (Queue) ===")

    var client = RedisClient("localhost", 6379)
    client.connect()

    # Build a task queue
    client.rpush("tasks", "process_order_1")
    client.rpush("tasks", "send_email_2")
    client.rpush("tasks", "generate_report_3")
    print("Added 3 tasks to queue")

    # Process tasks (FIFO)
    var task = client.lpop("tasks")
    print("Processing: " + task.as_string())

    # Check remaining
    var remaining = client.llen("tasks")
    print("Remaining tasks: " + String(remaining))

    # Get all remaining
    var all_tasks = client.lrange("tasks", 0, -1)
    print("Queue contents: " + all_tasks.as_string())

    client.close()
    print("")


fn pipeline_example() raises:
    """Batch multiple commands in one round trip."""
    print("=== Pipelining ===")

    var client = RedisClient("localhost", 6379)
    client.connect()

    # Create pipeline
    var pipeline = Pipeline(client)

    # Queue multiple commands
    pipeline.set("key1", "value1")
    pipeline.set("key2", "value2")
    pipeline.set("key3", "value3")
    pipeline.get("key1")
    pipeline.get("key2")
    pipeline.get("key3")

    # Execute all at once
    var results = pipeline.execute()
    print("Executed 6 commands in 1 round trip")
    print("Results: " + String(len(results)) + " responses")

    client.close()
    print("")


fn transaction_example() raises:
    """Atomic transactions with MULTI/EXEC."""
    print("=== Transactions ===")

    var client = RedisClient("localhost", 6379)
    client.connect()

    # Initialize balance
    client.set("balance", "100")

    # Atomic transfer
    var tx = Transaction(client)
    tx.decrby("balance", 25)  # Withdraw
    tx.set("last_withdrawal", "25")
    tx.incr("withdrawal_count")

    var results = tx.execute()
    print("Transaction executed atomically")

    var balance = client.get("balance")
    print("New balance: " + balance.as_string())

    client.close()
    print("")


fn main() raises:
    print("mojo-redis: Pure Mojo Redis Client (RESP Protocol)\n")
    print("Note: Requires Redis server on localhost:6379\n")

    basic_operations()
    hash_operations()
    list_operations()
    pipeline_example()
    transaction_example()

    print("=" * 50)
    print("Connection info:")
    print("  Default: localhost:6379")
    print("  Uses C FFI for TCP (no Python)")
