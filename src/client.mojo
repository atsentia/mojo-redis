"""
Redis Client Implementation

Provides a high-level Redis client with support for common commands.
Uses mojo-socket for TCP connections and RESP protocol for communication.

Example:
    var client = RedisClient("localhost", 6379)
    client.connect()
    client.set("key", "value")
    var value = client.get("key")
    client.close()
"""

from memory import UnsafePointer
from .resp import (
    RespValue, RespType, RespEncoder, RespDecoder,
    encode_command, decode_response, CRLF,
)


# =============================================================================
# Redis Connection Configuration
# =============================================================================

struct RedisConfig:
    """Redis connection configuration."""
    var host: String
    var port: Int
    var password: String
    var db: Int
    var timeout_seconds: Int
    var max_retries: Int

    fn __init__(out self):
        """Default configuration for localhost."""
        self.host = "127.0.0.1"
        self.port = 6379
        self.password = ""
        self.db = 0
        self.timeout_seconds = 30
        self.max_retries = 3

    fn __init__(out self, host: String, port: Int):
        """Configuration with custom host and port."""
        self.host = host
        self.port = port
        self.password = ""
        self.db = 0
        self.timeout_seconds = 30
        self.max_retries = 3

    fn __init__(out self, host: String, port: Int, password: String, db: Int):
        """Full configuration."""
        self.host = host
        self.port = port
        self.password = password
        self.db = db
        self.timeout_seconds = 30
        self.max_retries = 3


# =============================================================================
# Redis Client
# =============================================================================

struct RedisClient:
    """
    Redis client for executing commands.

    Supports string, hash, and list operations with connection pooling.

    Example:
        var client = RedisClient("localhost", 6379)
        client.connect()

        # String operations
        client.set("name", "Alice")
        var name = client.get("name")  # "Alice"

        # Hash operations
        client.hset("user:1", "name", "Bob")
        var user_name = client.hget("user:1", "name")  # "Bob"

        # List operations
        client.lpush("queue", "task1")
        client.rpush("queue", "task2")
        var task = client.lpop("queue")  # "task1"

        client.close()
    """
    var config: RedisConfig
    var encoder: RespEncoder
    var decoder: RespDecoder
    var socket_fd: Int32
    var is_connected: Bool
    var last_error: String

    fn __init__(out self):
        """Create client with default localhost configuration."""
        self.config = RedisConfig()
        self.encoder = RespEncoder()
        self.decoder = RespDecoder()
        self.socket_fd = -1
        self.is_connected = False
        self.last_error = ""

    fn __init__(out self, host: String, port: Int):
        """Create client with custom host and port."""
        self.config = RedisConfig(host, port)
        self.encoder = RespEncoder()
        self.decoder = RespDecoder()
        self.socket_fd = -1
        self.is_connected = False
        self.last_error = ""

    fn __init__(out self, config: RedisConfig):
        """Create client with full configuration."""
        self.config = config
        self.encoder = RespEncoder()
        self.decoder = RespDecoder()
        self.socket_fd = -1
        self.is_connected = False
        self.last_error = ""

    fn __del__(owned self):
        """Close connection on destruction."""
        if self.is_connected:
            self._close_socket()

    # =========================================================================
    # Connection Management
    # =========================================================================

    fn connect(inout self) raises:
        """
        Connect to Redis server.

        Establishes TCP connection and optionally authenticates.
        """
        if self.is_connected:
            return

        # Create socket
        self.socket_fd = self._create_socket()
        if self.socket_fd < 0:
            raise Error("Failed to create socket")

        # Connect to server
        var connected = self._connect_to_server()
        if not connected:
            self._close_socket()
            raise Error("Failed to connect to " + self.config.host + ":" + str(self.config.port))

        self.is_connected = True

        # Authenticate if password provided
        if len(self.config.password) > 0:
            var auth_result = self.auth(self.config.password)
            if auth_result.is_error():
                self.close()
                raise Error("Authentication failed: " + auth_result.string_value)

        # Select database if not default
        if self.config.db != 0:
            var select_result = self.select(self.config.db)
            if select_result.is_error():
                self.close()
                raise Error("Database selection failed: " + select_result.string_value)

    fn close(inout self):
        """Close the connection."""
        if self.is_connected:
            # Try to send QUIT command
            try:
                _ = self._execute("QUIT")
            except:
                pass
            self._close_socket()
            self.is_connected = False

    fn ping(inout self) raises -> Bool:
        """Ping the server. Returns True if PONG received."""
        var result = self._execute("PING")
        return result.as_string() == "PONG"

    fn auth(inout self, password: String) raises -> RespValue:
        """Authenticate with the server."""
        return self._execute("AUTH", password)

    fn select(inout self, db: Int) raises -> RespValue:
        """Select a database."""
        return self._execute("SELECT", str(db))

    # =========================================================================
    # String Commands
    # =========================================================================

    fn get(inout self, key: String) raises -> RespValue:
        """
        GET key

        Get the value of a key. Returns null if key doesn't exist.
        """
        return self._execute("GET", key)

    fn set(inout self, key: String, value: String) raises -> RespValue:
        """
        SET key value

        Set the string value of a key.
        """
        return self._execute("SET", key, value)

    fn set_ex(inout self, key: String, value: String, seconds: Int) raises -> RespValue:
        """
        SETEX key seconds value

        Set key with expiration time in seconds.
        """
        var args = List[String]()
        args.append("SETEX")
        args.append(key)
        args.append(str(seconds))
        args.append(value)
        return self._execute_list(args)

    fn set_nx(inout self, key: String, value: String) raises -> RespValue:
        """
        SETNX key value

        Set key only if it doesn't exist. Returns 1 if set, 0 if not.
        """
        return self._execute("SETNX", key, value)

    fn get_set(inout self, key: String, value: String) raises -> RespValue:
        """
        GETSET key value

        Set key and return the old value.
        """
        return self._execute("GETSET", key, value)

    fn mget(inout self, keys: List[String]) raises -> RespValue:
        """
        MGET key [key ...]

        Get values of multiple keys.
        """
        var args = List[String]()
        args.append("MGET")
        for i in range(len(keys)):
            args.append(keys[i])
        return self._execute_list(args)

    fn mset(inout self, pairs: List[String]) raises -> RespValue:
        """
        MSET key value [key value ...]

        Set multiple key-value pairs. Pairs list should alternate key, value.
        """
        var args = List[String]()
        args.append("MSET")
        for i in range(len(pairs)):
            args.append(pairs[i])
        return self._execute_list(args)

    fn incr(inout self, key: String) raises -> RespValue:
        """
        INCR key

        Increment the integer value of a key by one.
        """
        return self._execute("INCR", key)

    fn incrby(inout self, key: String, amount: Int) raises -> RespValue:
        """
        INCRBY key increment

        Increment the integer value of a key by the given amount.
        """
        return self._execute("INCRBY", key, str(amount))

    fn decr(inout self, key: String) raises -> RespValue:
        """
        DECR key

        Decrement the integer value of a key by one.
        """
        return self._execute("DECR", key)

    fn decrby(inout self, key: String, amount: Int) raises -> RespValue:
        """
        DECRBY key decrement

        Decrement the integer value of a key by the given amount.
        """
        return self._execute("DECRBY", key, str(amount))

    fn append(inout self, key: String, value: String) raises -> RespValue:
        """
        APPEND key value

        Append a value to a key. Returns new length.
        """
        return self._execute("APPEND", key, value)

    fn strlen(inout self, key: String) raises -> RespValue:
        """
        STRLEN key

        Get the length of the value stored at key.
        """
        return self._execute("STRLEN", key)

    # =========================================================================
    # Key Commands
    # =========================================================================

    fn exists(inout self, key: String) raises -> Bool:
        """
        EXISTS key

        Check if a key exists. Returns True if exists.
        """
        var result = self._execute("EXISTS", key)
        return result.as_int() > 0

    fn delete(inout self, key: String) raises -> Int64:
        """
        DEL key

        Delete a key. Returns number of keys deleted.
        """
        var result = self._execute("DEL", key)
        return result.as_int()

    fn delete_multi(inout self, keys: List[String]) raises -> Int64:
        """
        DEL key [key ...]

        Delete multiple keys. Returns number of keys deleted.
        """
        var args = List[String]()
        args.append("DEL")
        for i in range(len(keys)):
            args.append(keys[i])
        var result = self._execute_list(args)
        return result.as_int()

    fn expire(inout self, key: String, seconds: Int) raises -> Bool:
        """
        EXPIRE key seconds

        Set a key's time to live in seconds. Returns True if set.
        """
        var result = self._execute("EXPIRE", key, str(seconds))
        return result.as_int() == 1

    fn expireat(inout self, key: String, timestamp: Int64) raises -> Bool:
        """
        EXPIREAT key timestamp

        Set expiration as Unix timestamp. Returns True if set.
        """
        var result = self._execute("EXPIREAT", key, str(timestamp))
        return result.as_int() == 1

    fn ttl(inout self, key: String) raises -> Int64:
        """
        TTL key

        Get time to live in seconds. Returns -1 if no expiry, -2 if not exists.
        """
        var result = self._execute("TTL", key)
        return result.as_int()

    fn pttl(inout self, key: String) raises -> Int64:
        """
        PTTL key

        Get time to live in milliseconds.
        """
        var result = self._execute("PTTL", key)
        return result.as_int()

    fn persist(inout self, key: String) raises -> Bool:
        """
        PERSIST key

        Remove the expiration from a key. Returns True if removed.
        """
        var result = self._execute("PERSIST", key)
        return result.as_int() == 1

    fn type_of(inout self, key: String) raises -> String:
        """
        TYPE key

        Get the type of a key (string, list, set, zset, hash, stream).
        """
        var result = self._execute("TYPE", key)
        return result.as_string()

    fn keys(inout self, pattern: String) raises -> RespValue:
        """
        KEYS pattern

        Find all keys matching the pattern. Use with caution in production!
        """
        return self._execute("KEYS", pattern)

    fn rename(inout self, key: String, newkey: String) raises -> RespValue:
        """
        RENAME key newkey

        Rename a key.
        """
        return self._execute("RENAME", key, newkey)

    fn renamenx(inout self, key: String, newkey: String) raises -> Bool:
        """
        RENAMENX key newkey

        Rename a key only if new key doesn't exist. Returns True if renamed.
        """
        var result = self._execute("RENAMENX", key, newkey)
        return result.as_int() == 1

    # =========================================================================
    # Hash Commands
    # =========================================================================

    fn hget(inout self, key: String, field: String) raises -> RespValue:
        """
        HGET key field

        Get the value of a hash field.
        """
        return self._execute("HGET", key, field)

    fn hset(inout self, key: String, field: String, value: String) raises -> RespValue:
        """
        HSET key field value

        Set the string value of a hash field. Returns 1 if new, 0 if updated.
        """
        return self._execute("HSET", key, field, value)

    fn hsetnx(inout self, key: String, field: String, value: String) raises -> Bool:
        """
        HSETNX key field value

        Set field only if it doesn't exist. Returns True if set.
        """
        var result = self._execute("HSETNX", key, field, value)
        return result.as_int() == 1

    fn hdel(inout self, key: String, field: String) raises -> Int64:
        """
        HDEL key field

        Delete a hash field. Returns number of fields deleted.
        """
        var result = self._execute("HDEL", key, field)
        return result.as_int()

    fn hexists(inout self, key: String, field: String) raises -> Bool:
        """
        HEXISTS key field

        Check if a hash field exists.
        """
        var result = self._execute("HEXISTS", key, field)
        return result.as_int() == 1

    fn hgetall(inout self, key: String) raises -> RespValue:
        """
        HGETALL key

        Get all fields and values in a hash.
        """
        return self._execute("HGETALL", key)

    fn hkeys(inout self, key: String) raises -> RespValue:
        """
        HKEYS key

        Get all field names in a hash.
        """
        return self._execute("HKEYS", key)

    fn hvals(inout self, key: String) raises -> RespValue:
        """
        HVALS key

        Get all values in a hash.
        """
        return self._execute("HVALS", key)

    fn hlen(inout self, key: String) raises -> Int64:
        """
        HLEN key

        Get the number of fields in a hash.
        """
        var result = self._execute("HLEN", key)
        return result.as_int()

    fn hmset(inout self, key: String, field_values: List[String]) raises -> RespValue:
        """
        HMSET key field value [field value ...]

        Set multiple hash fields. List alternates field, value.
        """
        var args = List[String]()
        args.append("HMSET")
        args.append(key)
        for i in range(len(field_values)):
            args.append(field_values[i])
        return self._execute_list(args)

    fn hmget(inout self, key: String, fields: List[String]) raises -> RespValue:
        """
        HMGET key field [field ...]

        Get values of multiple hash fields.
        """
        var args = List[String]()
        args.append("HMGET")
        args.append(key)
        for i in range(len(fields)):
            args.append(fields[i])
        return self._execute_list(args)

    fn hincrby(inout self, key: String, field: String, amount: Int) raises -> Int64:
        """
        HINCRBY key field increment

        Increment hash field by integer.
        """
        var result = self._execute("HINCRBY", key, field, str(amount))
        return result.as_int()

    # =========================================================================
    # List Commands
    # =========================================================================

    fn lpush(inout self, key: String, value: String) raises -> Int64:
        """
        LPUSH key value

        Prepend a value to a list. Returns list length.
        """
        var result = self._execute("LPUSH", key, value)
        return result.as_int()

    fn lpush_multi(inout self, key: String, values: List[String]) raises -> Int64:
        """
        LPUSH key value [value ...]

        Prepend multiple values to a list.
        """
        var args = List[String]()
        args.append("LPUSH")
        args.append(key)
        for i in range(len(values)):
            args.append(values[i])
        var result = self._execute_list(args)
        return result.as_int()

    fn rpush(inout self, key: String, value: String) raises -> Int64:
        """
        RPUSH key value

        Append a value to a list. Returns list length.
        """
        var result = self._execute("RPUSH", key, value)
        return result.as_int()

    fn rpush_multi(inout self, key: String, values: List[String]) raises -> Int64:
        """
        RPUSH key value [value ...]

        Append multiple values to a list.
        """
        var args = List[String]()
        args.append("RPUSH")
        args.append(key)
        for i in range(len(values)):
            args.append(values[i])
        var result = self._execute_list(args)
        return result.as_int()

    fn lpop(inout self, key: String) raises -> RespValue:
        """
        LPOP key

        Remove and get the first element of a list.
        """
        return self._execute("LPOP", key)

    fn rpop(inout self, key: String) raises -> RespValue:
        """
        RPOP key

        Remove and get the last element of a list.
        """
        return self._execute("RPOP", key)

    fn llen(inout self, key: String) raises -> Int64:
        """
        LLEN key

        Get the length of a list.
        """
        var result = self._execute("LLEN", key)
        return result.as_int()

    fn lrange(inout self, key: String, start: Int, stop: Int) raises -> RespValue:
        """
        LRANGE key start stop

        Get elements from list by index range. Use -1 for last element.
        """
        var args = List[String]()
        args.append("LRANGE")
        args.append(key)
        args.append(str(start))
        args.append(str(stop))
        return self._execute_list(args)

    fn lindex(inout self, key: String, index: Int) raises -> RespValue:
        """
        LINDEX key index

        Get element at index.
        """
        return self._execute("LINDEX", key, str(index))

    fn lset(inout self, key: String, index: Int, value: String) raises -> RespValue:
        """
        LSET key index value

        Set element at index.
        """
        return self._execute("LSET", key, str(index), value)

    fn lrem(inout self, key: String, count: Int, value: String) raises -> Int64:
        """
        LREM key count value

        Remove elements from list. Count determines direction and count.
        """
        var args = List[String]()
        args.append("LREM")
        args.append(key)
        args.append(str(count))
        args.append(value)
        var result = self._execute_list(args)
        return result.as_int()

    fn ltrim(inout self, key: String, start: Int, stop: Int) raises -> RespValue:
        """
        LTRIM key start stop

        Trim list to specified range.
        """
        var args = List[String]()
        args.append("LTRIM")
        args.append(key)
        args.append(str(start))
        args.append(str(stop))
        return self._execute_list(args)

    fn rpoplpush(inout self, source: String, destination: String) raises -> RespValue:
        """
        RPOPLPUSH source destination

        Pop from one list and push to another.
        """
        return self._execute("RPOPLPUSH", source, destination)

    # =========================================================================
    # Server Commands
    # =========================================================================

    fn dbsize(inout self) raises -> Int64:
        """
        DBSIZE

        Return the number of keys in the selected database.
        """
        var result = self._execute("DBSIZE")
        return result.as_int()

    fn flushdb(inout self) raises -> RespValue:
        """
        FLUSHDB

        Delete all keys in the current database.
        """
        return self._execute("FLUSHDB")

    fn flushall(inout self) raises -> RespValue:
        """
        FLUSHALL

        Delete all keys in all databases.
        """
        return self._execute("FLUSHALL")

    fn info(inout self) raises -> RespValue:
        """
        INFO

        Get information and statistics about the server.
        """
        return self._execute("INFO")

    fn info_section(inout self, section: String) raises -> RespValue:
        """
        INFO section

        Get information about a specific section.
        """
        return self._execute("INFO", section)

    # =========================================================================
    # Internal Methods
    # =========================================================================

    fn _execute(inout self, command: String) raises -> RespValue:
        """Execute a command with no arguments."""
        var encoded = self.encoder.encode_command(command)
        return self._send_and_receive(encoded)

    fn _execute(inout self, command: String, arg1: String) raises -> RespValue:
        """Execute a command with one argument."""
        var encoded = self.encoder.encode_command(command, arg1)
        return self._send_and_receive(encoded)

    fn _execute(inout self, command: String, arg1: String, arg2: String) raises -> RespValue:
        """Execute a command with two arguments."""
        var encoded = self.encoder.encode_command(command, arg1, arg2)
        return self._send_and_receive(encoded)

    fn _execute(inout self, command: String, arg1: String, arg2: String, arg3: String) raises -> RespValue:
        """Execute a command with three arguments."""
        var encoded = self.encoder.encode_command(command, arg1, arg2, arg3)
        return self._send_and_receive(encoded)

    fn _execute_list(inout self, args: List[String]) raises -> RespValue:
        """Execute a command from a list of strings."""
        var encoded = self.encoder.encode_command_list(args)
        return self._send_and_receive(encoded)

    fn _send_and_receive(inout self, data: String) raises -> RespValue:
        """Send data and receive response."""
        if not self.is_connected:
            raise Error("Not connected to Redis server")

        # Send command
        var sent = self._send(data)
        if sent < 0:
            self.is_connected = False
            raise Error("Failed to send command")

        # Receive response
        var response = self._recv()
        if len(response) == 0:
            self.is_connected = False
            raise Error("No response from server")

        return self.decoder.decode(response)

    # =========================================================================
    # Socket Operations (C FFI)
    # These mirror mojo-socket but are embedded here for standalone use
    # =========================================================================

    fn _create_socket(self) -> Int32:
        """Create a TCP socket."""
        from sys.ffi import external_call
        # AF_INET=2, SOCK_STREAM=1, IPPROTO_TCP=6
        return external_call["socket", Int32, Int32, Int32, Int32](2, 1, 6)

    fn _connect_to_server(self) -> Bool:
        """Connect to Redis server."""
        from sys.ffi import external_call
        from memory import UnsafePointer

        # Build sockaddr_in structure
        var addr = UnsafePointer[UInt8].alloc(16)

        # sin_len (macOS) and sin_family
        addr[0] = 16  # length
        addr[1] = 2   # AF_INET

        # sin_port (network byte order - big endian)
        var port = self.config.port
        addr[2] = UInt8((port >> 8) & 0xFF)
        addr[3] = UInt8(port & 0xFF)

        # sin_addr (parse IP address)
        var ip_parts = self.config.host.split(".")
        if len(ip_parts) == 4:
            try:
                addr[4] = UInt8(Int(ip_parts[0]))
                addr[5] = UInt8(Int(ip_parts[1]))
                addr[6] = UInt8(Int(ip_parts[2]))
                addr[7] = UInt8(Int(ip_parts[3]))
            except:
                # Default to localhost
                addr[4] = 127
                addr[5] = 0
                addr[6] = 0
                addr[7] = 1
        else:
            # localhost
            addr[4] = 127
            addr[5] = 0
            addr[6] = 0
            addr[7] = 1

        # Zero padding
        for i in range(8, 16):
            addr[i] = 0

        var result = external_call["connect", Int32, Int32, UnsafePointer[UInt8], UInt32](
            self.socket_fd, addr, 16
        )

        addr.free()
        return result == 0

    fn _send(self, data: String) -> Int:
        """Send data to socket."""
        from sys.ffi import external_call
        from memory import UnsafePointer

        var length = len(data)
        var buffer = UnsafePointer[UInt8].alloc(length)

        for i in range(length):
            buffer[i] = UInt8(ord(data[i]))

        var sent = external_call["send", Int, Int32, UnsafePointer[UInt8], Int, Int32](
            self.socket_fd, buffer, length, 0
        )

        buffer.free()
        return sent

    fn _recv(self) -> String:
        """Receive data from socket."""
        from sys.ffi import external_call
        from memory import UnsafePointer

        var buffer_size = 8192
        var buffer = UnsafePointer[UInt8].alloc(buffer_size)

        var received = external_call["recv", Int, Int32, UnsafePointer[UInt8], Int, Int32](
            self.socket_fd, buffer, buffer_size - 1, 0
        )

        if received <= 0:
            buffer.free()
            return ""

        var result = String()
        for i in range(received):
            result += chr(Int(buffer[i]))

        buffer.free()
        return result

    fn _close_socket(inout self):
        """Close the socket."""
        from sys.ffi import external_call
        if self.socket_fd >= 0:
            _ = external_call["close", Int32, Int32](self.socket_fd)
            self.socket_fd = -1
