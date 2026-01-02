"""
Redis Pipeline Implementation

Provides command pipelining for batch operations.
Pipelining sends multiple commands without waiting for responses,
then reads all responses at once - significantly improving throughput.

Example:
    var client = RedisClient("localhost", 6379)
    client.connect()

    var pipeline = Pipeline(client)
    pipeline.set("key1", "value1")
    pipeline.set("key2", "value2")
    pipeline.get("key1")
    pipeline.get("key2")

    var results = pipeline.execute()
    # results contains 4 RespValue responses
"""

from .resp import RespValue, RespEncoder, RespDecoder, CRLF
from .client import RedisClient


# =============================================================================
# Pipeline Command
# =============================================================================

struct PipelineCommand:
    """Stores a single pipelined command."""
    var encoded: String
    var command_name: String

    fn __init__(out self, encoded: String, command_name: String):
        self.encoded = encoded
        self.command_name = command_name

    fn __copyinit__(out self, other: PipelineCommand):
        self.encoded = other.encoded
        self.command_name = other.command_name

    fn __moveinit__(out self, owned other: PipelineCommand):
        self.encoded = other.encoded^
        self.command_name = other.command_name^


# =============================================================================
# Pipeline
# =============================================================================

struct Pipeline:
    """
    Redis command pipeline for batch operations.

    Pipelining reduces network round-trip overhead by sending multiple
    commands in a single write, then reading all responses together.

    Example:
        var pipeline = Pipeline(client)

        # Queue up commands (no network I/O yet)
        pipeline.set("counter", "0")
        pipeline.incr("counter")
        pipeline.incr("counter")
        pipeline.get("counter")

        # Execute all at once
        var results = pipeline.execute()
        # results[0] = OK (from SET)
        # results[1] = 1 (from first INCR)
        # results[2] = 2 (from second INCR)
        # results[3] = "2" (from GET)

    Performance:
        Without pipelining: N commands = N round trips
        With pipelining: N commands = 1 round trip
    """
    var client: UnsafePointer[RedisClient]
    var commands: List[PipelineCommand]
    var encoder: RespEncoder

    fn __init__(out self, client: UnsafePointer[RedisClient]):
        """Create pipeline attached to a client."""
        self.client = client
        self.commands = List[PipelineCommand]()
        self.encoder = RespEncoder()

    fn __del__(owned self):
        """Destructor - does not own client."""
        pass

    fn size(self) -> Int:
        """Get number of queued commands."""
        return len(self.commands)

    fn clear(inout self):
        """Clear all queued commands."""
        self.commands = List[PipelineCommand]()

    # =========================================================================
    # String Commands
    # =========================================================================

    fn get(inout self, key: String):
        """Queue GET command."""
        var encoded = self.encoder.encode_command("GET", key)
        self.commands.append(PipelineCommand(encoded, "GET"))

    fn set(inout self, key: String, value: String):
        """Queue SET command."""
        var encoded = self.encoder.encode_command("SET", key, value)
        self.commands.append(PipelineCommand(encoded, "SET"))

    fn set_ex(inout self, key: String, value: String, seconds: Int):
        """Queue SETEX command."""
        var args = List[String]()
        args.append("SETEX")
        args.append(key)
        args.append(str(seconds))
        args.append(value)
        var encoded = self.encoder.encode_command_list(args)
        self.commands.append(PipelineCommand(encoded, "SETEX"))

    fn incr(inout self, key: String):
        """Queue INCR command."""
        var encoded = self.encoder.encode_command("INCR", key)
        self.commands.append(PipelineCommand(encoded, "INCR"))

    fn incrby(inout self, key: String, amount: Int):
        """Queue INCRBY command."""
        var encoded = self.encoder.encode_command("INCRBY", key, str(amount))
        self.commands.append(PipelineCommand(encoded, "INCRBY"))

    fn decr(inout self, key: String):
        """Queue DECR command."""
        var encoded = self.encoder.encode_command("DECR", key)
        self.commands.append(PipelineCommand(encoded, "DECR"))

    fn decrby(inout self, key: String, amount: Int):
        """Queue DECRBY command."""
        var encoded = self.encoder.encode_command("DECRBY", key, str(amount))
        self.commands.append(PipelineCommand(encoded, "DECRBY"))

    fn append_str(inout self, key: String, value: String):
        """Queue APPEND command."""
        var encoded = self.encoder.encode_command("APPEND", key, value)
        self.commands.append(PipelineCommand(encoded, "APPEND"))

    # =========================================================================
    # Key Commands
    # =========================================================================

    fn delete(inout self, key: String):
        """Queue DEL command."""
        var encoded = self.encoder.encode_command("DEL", key)
        self.commands.append(PipelineCommand(encoded, "DEL"))

    fn exists(inout self, key: String):
        """Queue EXISTS command."""
        var encoded = self.encoder.encode_command("EXISTS", key)
        self.commands.append(PipelineCommand(encoded, "EXISTS"))

    fn expire(inout self, key: String, seconds: Int):
        """Queue EXPIRE command."""
        var encoded = self.encoder.encode_command("EXPIRE", key, str(seconds))
        self.commands.append(PipelineCommand(encoded, "EXPIRE"))

    fn ttl(inout self, key: String):
        """Queue TTL command."""
        var encoded = self.encoder.encode_command("TTL", key)
        self.commands.append(PipelineCommand(encoded, "TTL"))

    fn type_of(inout self, key: String):
        """Queue TYPE command."""
        var encoded = self.encoder.encode_command("TYPE", key)
        self.commands.append(PipelineCommand(encoded, "TYPE"))

    # =========================================================================
    # Hash Commands
    # =========================================================================

    fn hget(inout self, key: String, field: String):
        """Queue HGET command."""
        var encoded = self.encoder.encode_command("HGET", key, field)
        self.commands.append(PipelineCommand(encoded, "HGET"))

    fn hset(inout self, key: String, field: String, value: String):
        """Queue HSET command."""
        var encoded = self.encoder.encode_command("HSET", key, field, value)
        self.commands.append(PipelineCommand(encoded, "HSET"))

    fn hdel(inout self, key: String, field: String):
        """Queue HDEL command."""
        var encoded = self.encoder.encode_command("HDEL", key, field)
        self.commands.append(PipelineCommand(encoded, "HDEL"))

    fn hexists(inout self, key: String, field: String):
        """Queue HEXISTS command."""
        var encoded = self.encoder.encode_command("HEXISTS", key, field)
        self.commands.append(PipelineCommand(encoded, "HEXISTS"))

    fn hgetall(inout self, key: String):
        """Queue HGETALL command."""
        var encoded = self.encoder.encode_command("HGETALL", key)
        self.commands.append(PipelineCommand(encoded, "HGETALL"))

    fn hkeys(inout self, key: String):
        """Queue HKEYS command."""
        var encoded = self.encoder.encode_command("HKEYS", key)
        self.commands.append(PipelineCommand(encoded, "HKEYS"))

    fn hvals(inout self, key: String):
        """Queue HVALS command."""
        var encoded = self.encoder.encode_command("HVALS", key)
        self.commands.append(PipelineCommand(encoded, "HVALS"))

    fn hlen(inout self, key: String):
        """Queue HLEN command."""
        var encoded = self.encoder.encode_command("HLEN", key)
        self.commands.append(PipelineCommand(encoded, "HLEN"))

    fn hincrby(inout self, key: String, field: String, amount: Int):
        """Queue HINCRBY command."""
        var encoded = self.encoder.encode_command("HINCRBY", key, field, str(amount))
        self.commands.append(PipelineCommand(encoded, "HINCRBY"))

    # =========================================================================
    # List Commands
    # =========================================================================

    fn lpush(inout self, key: String, value: String):
        """Queue LPUSH command."""
        var encoded = self.encoder.encode_command("LPUSH", key, value)
        self.commands.append(PipelineCommand(encoded, "LPUSH"))

    fn rpush(inout self, key: String, value: String):
        """Queue RPUSH command."""
        var encoded = self.encoder.encode_command("RPUSH", key, value)
        self.commands.append(PipelineCommand(encoded, "RPUSH"))

    fn lpop(inout self, key: String):
        """Queue LPOP command."""
        var encoded = self.encoder.encode_command("LPOP", key)
        self.commands.append(PipelineCommand(encoded, "LPOP"))

    fn rpop(inout self, key: String):
        """Queue RPOP command."""
        var encoded = self.encoder.encode_command("RPOP", key)
        self.commands.append(PipelineCommand(encoded, "RPOP"))

    fn llen(inout self, key: String):
        """Queue LLEN command."""
        var encoded = self.encoder.encode_command("LLEN", key)
        self.commands.append(PipelineCommand(encoded, "LLEN"))

    fn lrange(inout self, key: String, start: Int, stop: Int):
        """Queue LRANGE command."""
        var args = List[String]()
        args.append("LRANGE")
        args.append(key)
        args.append(str(start))
        args.append(str(stop))
        var encoded = self.encoder.encode_command_list(args)
        self.commands.append(PipelineCommand(encoded, "LRANGE"))

    fn lindex(inout self, key: String, index: Int):
        """Queue LINDEX command."""
        var encoded = self.encoder.encode_command("LINDEX", key, str(index))
        self.commands.append(PipelineCommand(encoded, "LINDEX"))

    fn lset(inout self, key: String, index: Int, value: String):
        """Queue LSET command."""
        var encoded = self.encoder.encode_command("LSET", key, str(index), value)
        self.commands.append(PipelineCommand(encoded, "LSET"))

    # =========================================================================
    # Execution
    # =========================================================================

    fn execute(inout self) raises -> List[RespValue]:
        """
        Execute all queued commands and return responses.

        Sends all commands in a single batch, then reads all responses.
        Returns a list of RespValue, one per command in queue order.
        """
        if len(self.commands) == 0:
            return List[RespValue]()

        # Build combined payload
        var payload = String()
        for i in range(len(self.commands)):
            payload += self.commands[i].encoded

        # Send all at once
        var sent = self._send(payload)
        if sent < 0:
            raise Error("Failed to send pipeline commands")

        # Receive and parse all responses
        var results = List[RespValue]()
        var remaining_count = len(self.commands)
        var decoder = RespDecoder()
        var buffer = String()

        while remaining_count > 0:
            var data = self._recv()
            if len(data) == 0:
                raise Error("Connection closed while reading pipeline responses")

            buffer += data

            # Try to parse complete responses from buffer
            while remaining_count > 0:
                var parsed = self._try_parse_response(buffer, decoder)
                if parsed.is_null and len(buffer) > 0 and buffer[0] != "$" and buffer[0] != "*":
                    # Might be incomplete, need more data
                    if not self._has_complete_response(buffer):
                        break
                    # Actually null response
                    results.append(parsed)
                    remaining_count -= 1
                elif parsed.is_null and (len(buffer) == 0 or not self._has_complete_response(buffer)):
                    # Need more data
                    break
                else:
                    results.append(parsed)
                    remaining_count -= 1

        # Clear the pipeline
        self.clear()

        return results

    fn _has_complete_response(self, data: String) -> Bool:
        """Check if buffer contains at least one complete RESP response."""
        if len(data) == 0:
            return False

        var prefix = data[0]

        # Simple string, error, integer - look for CRLF
        if prefix == "+" or prefix == "-" or prefix == ":":
            return self._find_crlf(data, 1) >= 0

        # Bulk string
        if prefix == "$":
            var crlf_pos = self._find_crlf(data, 1)
            if crlf_pos < 0:
                return False
            var length_str = data[1:crlf_pos]
            try:
                var length = Int(length_str)
                if length < 0:
                    return True  # Null bulk string
                # Need: $<len>\r\n<data>\r\n
                var total_needed = crlf_pos + 2 + length + 2
                return len(data) >= total_needed
            except:
                return False

        # Array
        if prefix == "*":
            # For simplicity, assume complete if we have enough CRLFs
            # A proper implementation would recursively check each element
            var crlf_pos = self._find_crlf(data, 1)
            if crlf_pos < 0:
                return False
            return True  # Simplified - let parser handle it

        return True

    fn _find_crlf(self, data: String, start: Int) -> Int:
        """Find position of \r\n starting from index."""
        var i = start
        while i < len(data) - 1:
            if data[i] == "\r" and data[i + 1] == "\n":
                return i
            i += 1
        return -1

    fn _try_parse_response(inout self, inout buffer: String, inout decoder: RespDecoder) -> RespValue:
        """Try to parse one response from buffer, removing consumed bytes."""
        if len(buffer) == 0:
            return RespValue.null()

        # Find end of this response
        var response_end = self._find_response_end(buffer)
        if response_end < 0:
            return RespValue.null()

        var response_data = buffer[:response_end]
        buffer = buffer[response_end:]

        return decoder.decode(response_data)

    fn _find_response_end(self, data: String) -> Int:
        """Find the end index of the first complete response in data."""
        if len(data) == 0:
            return -1

        var prefix = data[0]

        # Simple string, error, integer
        if prefix == "+" or prefix == "-" or prefix == ":":
            var crlf = self._find_crlf(data, 1)
            if crlf >= 0:
                return crlf + 2
            return -1

        # Bulk string
        if prefix == "$":
            var crlf = self._find_crlf(data, 1)
            if crlf < 0:
                return -1
            var length_str = data[1:crlf]
            try:
                var length = Int(length_str)
                if length < 0:
                    return crlf + 2  # $-1\r\n
                var end = crlf + 2 + length + 2
                if end <= len(data):
                    return end
                return -1
            except:
                return -1

        # Array - simplified handling
        if prefix == "*":
            var crlf = self._find_crlf(data, 1)
            if crlf < 0:
                return -1
            var count_str = data[1:crlf]
            try:
                var count = Int(count_str)
                if count < 0:
                    return crlf + 2  # *-1\r\n

                var pos = crlf + 2
                for _ in range(count):
                    if pos >= len(data):
                        return -1
                    var elem_end = self._find_element_end(data, pos)
                    if elem_end < 0:
                        return -1
                    pos = elem_end
                return pos
            except:
                return -1

        return -1

    fn _find_element_end(self, data: String, start: Int) -> Int:
        """Find end of RESP element starting at position."""
        if start >= len(data):
            return -1

        var prefix = data[start]

        if prefix == "+" or prefix == "-" or prefix == ":":
            var crlf = self._find_crlf(data, start + 1)
            if crlf >= 0:
                return crlf + 2
            return -1

        if prefix == "$":
            var crlf = self._find_crlf(data, start + 1)
            if crlf < 0:
                return -1
            var length_str = data[start + 1:crlf]
            try:
                var length = Int(length_str)
                if length < 0:
                    return crlf + 2
                var end = crlf + 2 + length + 2
                if end <= len(data):
                    return end
                return -1
            except:
                return -1

        if prefix == "*":
            # Recursive array
            var crlf = self._find_crlf(data, start + 1)
            if crlf < 0:
                return -1
            var count_str = data[start + 1:crlf]
            try:
                var count = Int(count_str)
                if count < 0:
                    return crlf + 2
                var pos = crlf + 2
                for _ in range(count):
                    var elem_end = self._find_element_end(data, pos)
                    if elem_end < 0:
                        return -1
                    pos = elem_end
                return pos
            except:
                return -1

        return -1

    # =========================================================================
    # Socket Operations (mirrors client)
    # =========================================================================

    fn _send(self, data: String) -> Int:
        """Send data through client's socket."""
        from sys.ffi import external_call
        from memory import UnsafePointer

        var socket_fd = self.client[].socket_fd
        var length = len(data)
        var buffer = UnsafePointer[UInt8].alloc(length)

        for i in range(length):
            buffer[i] = UInt8(ord(data[i]))

        var sent = external_call["send", Int, Int32, UnsafePointer[UInt8], Int, Int32](
            socket_fd, buffer, length, 0
        )

        buffer.free()
        return sent

    fn _recv(self) -> String:
        """Receive data through client's socket."""
        from sys.ffi import external_call
        from memory import UnsafePointer

        var socket_fd = self.client[].socket_fd
        var buffer_size = 65536  # Larger buffer for pipelined responses
        var buffer = UnsafePointer[UInt8].alloc(buffer_size)

        var received = external_call["recv", Int, Int32, UnsafePointer[UInt8], Int, Int32](
            socket_fd, buffer, buffer_size - 1, 0
        )

        if received <= 0:
            buffer.free()
            return ""

        var result = String()
        for i in range(received):
            result += chr(Int(buffer[i]))

        buffer.free()
        return result


# =============================================================================
# Transaction Pipeline (MULTI/EXEC)
# =============================================================================

struct Transaction:
    """
    Redis transaction for atomic operations.

    Uses MULTI/EXEC to ensure all commands execute atomically.

    Example:
        var tx = Transaction(client)
        tx.set("key1", "value1")
        tx.incr("counter")
        var results = tx.execute()
        # All commands executed atomically
    """
    var pipeline: Pipeline

    fn __init__(out self, client: UnsafePointer[RedisClient]):
        """Create transaction attached to a client."""
        self.pipeline = Pipeline(client)
        # Queue MULTI command first
        var encoder = RespEncoder()
        var multi_cmd = encoder.encode_command("MULTI")
        self.pipeline.commands.append(PipelineCommand(multi_cmd, "MULTI"))

    fn size(self) -> Int:
        """Get number of queued commands (excluding MULTI/EXEC)."""
        return len(self.pipeline.commands) - 1  # -1 for MULTI

    # Forward all methods to pipeline
    fn get(inout self, key: String):
        self.pipeline.get(key)

    fn set(inout self, key: String, value: String):
        self.pipeline.set(key, value)

    fn incr(inout self, key: String):
        self.pipeline.incr(key)

    fn decr(inout self, key: String):
        self.pipeline.decr(key)

    fn delete(inout self, key: String):
        self.pipeline.delete(key)

    fn expire(inout self, key: String, seconds: Int):
        self.pipeline.expire(key, seconds)

    fn hget(inout self, key: String, field: String):
        self.pipeline.hget(key, field)

    fn hset(inout self, key: String, field: String, value: String):
        self.pipeline.hset(key, field, value)

    fn lpush(inout self, key: String, value: String):
        self.pipeline.lpush(key, value)

    fn rpush(inout self, key: String, value: String):
        self.pipeline.rpush(key, value)

    fn lpop(inout self, key: String):
        self.pipeline.lpop(key)

    fn rpop(inout self, key: String):
        self.pipeline.rpop(key)

    fn execute(inout self) raises -> List[RespValue]:
        """
        Execute transaction atomically.

        Returns the results from EXEC (array of command results).
        """
        # Add EXEC command
        var encoder = RespEncoder()
        var exec_cmd = encoder.encode_command("EXEC")
        self.pipeline.commands.append(PipelineCommand(exec_cmd, "EXEC"))

        # Execute all (MULTI + commands + EXEC)
        var results = self.pipeline.execute()

        # Results structure:
        # [0] = OK (from MULTI)
        # [1..n-1] = QUEUED (from each command)
        # [n] = Array of actual results (from EXEC)

        if len(results) > 0:
            var last = results[len(results) - 1]
            if last.is_array():
                return last.array_value
            # Transaction failed (EXEC returned error or null)
            if last.is_error():
                raise Error("Transaction failed: " + last.string_value)
            if last.is_null:
                raise Error("Transaction aborted (watched key modified)")

        return List[RespValue]()

    fn discard(inout self) raises:
        """Discard queued commands (DISCARD)."""
        # Clear pipeline and send DISCARD
        self.pipeline.clear()
        var encoder = RespEncoder()
        var discard_cmd = encoder.encode_command("DISCARD")
        self.pipeline.commands.append(PipelineCommand(discard_cmd, "DISCARD"))
        _ = self.pipeline.execute()
