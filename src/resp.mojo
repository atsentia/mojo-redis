"""
RESP (Redis Serialization Protocol) Parser

Implements RESP2/RESP3 protocol encoding and decoding for Redis communication.

RESP Types:
- Simple Strings: +OK\r\n
- Errors: -ERR message\r\n
- Integers: :1000\r\n
- Bulk Strings: $6\r\nfoobar\r\n
- Arrays: *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
- Null Bulk String: $-1\r\n
- Null Array: *-1\r\n
"""


# =============================================================================
# Constants
# =============================================================================

alias CRLF: String = "\r\n"
alias SIMPLE_STRING_PREFIX: String = "+"
alias ERROR_PREFIX: String = "-"
alias INTEGER_PREFIX: String = ":"
alias BULK_STRING_PREFIX: String = "$"
alias ARRAY_PREFIX: String = "*"


# =============================================================================
# RESP Value Types
# =============================================================================

@value
struct RespType:
    """RESP value type enumeration."""
    var value: Int

    alias SIMPLE_STRING = RespType(0)
    alias ERROR = RespType(1)
    alias INTEGER = RespType(2)
    alias BULK_STRING = RespType(3)
    alias ARRAY = RespType(4)
    alias NULL = RespType(5)

    fn __eq__(self, other: RespType) -> Bool:
        return self.value == other.value

    fn __ne__(self, other: RespType) -> Bool:
        return self.value != other.value


# =============================================================================
# RESP Value
# =============================================================================

struct RespValue:
    """
    Represents a RESP protocol value.

    Can hold strings, integers, arrays, errors, or null.
    """
    var resp_type: RespType
    var string_value: String
    var int_value: Int64
    var array_value: List[RespValue]
    var is_null: Bool

    fn __init__(out self):
        """Create a null value."""
        self.resp_type = RespType.NULL
        self.string_value = ""
        self.int_value = 0
        self.array_value = List[RespValue]()
        self.is_null = True

    fn __init__(out self, value: String):
        """Create a bulk string value."""
        self.resp_type = RespType.BULK_STRING
        self.string_value = value
        self.int_value = 0
        self.array_value = List[RespValue]()
        self.is_null = False

    fn __init__(out self, value: Int64):
        """Create an integer value."""
        self.resp_type = RespType.INTEGER
        self.string_value = ""
        self.int_value = value
        self.array_value = List[RespValue]()
        self.is_null = False

    fn __init__(out self, value: Int):
        """Create an integer value from Int."""
        self.resp_type = RespType.INTEGER
        self.string_value = ""
        self.int_value = Int64(value)
        self.array_value = List[RespValue]()
        self.is_null = False

    fn __copyinit__(out self, other: RespValue):
        """Copy constructor."""
        self.resp_type = other.resp_type
        self.string_value = other.string_value
        self.int_value = other.int_value
        self.array_value = List[RespValue]()
        for i in range(len(other.array_value)):
            self.array_value.append(other.array_value[i])
        self.is_null = other.is_null

    fn __moveinit__(out self, owned other: RespValue):
        """Move constructor."""
        self.resp_type = other.resp_type
        self.string_value = other.string_value^
        self.int_value = other.int_value
        self.array_value = other.array_value^
        self.is_null = other.is_null

    @staticmethod
    fn null() -> RespValue:
        """Create a null value."""
        return RespValue()

    @staticmethod
    fn simple_string(value: String) -> RespValue:
        """Create a simple string value."""
        var result = RespValue(value)
        result.resp_type = RespType.SIMPLE_STRING
        return result

    @staticmethod
    fn error(message: String) -> RespValue:
        """Create an error value."""
        var result = RespValue(message)
        result.resp_type = RespType.ERROR
        return result

    @staticmethod
    fn array(values: List[RespValue]) -> RespValue:
        """Create an array value."""
        var result = RespValue()
        result.resp_type = RespType.ARRAY
        result.is_null = False
        result.array_value = values
        return result

    fn is_error(self) -> Bool:
        """Check if this is an error response."""
        return self.resp_type == RespType.ERROR

    fn is_string(self) -> Bool:
        """Check if this is a string (simple or bulk)."""
        return self.resp_type == RespType.SIMPLE_STRING or self.resp_type == RespType.BULK_STRING

    fn is_integer(self) -> Bool:
        """Check if this is an integer."""
        return self.resp_type == RespType.INTEGER

    fn is_array(self) -> Bool:
        """Check if this is an array."""
        return self.resp_type == RespType.ARRAY

    fn as_string(self) -> String:
        """Get value as string."""
        if self.is_null:
            return ""
        if self.resp_type == RespType.INTEGER:
            return str(self.int_value)
        return self.string_value

    fn as_int(self) -> Int64:
        """Get value as integer."""
        if self.resp_type == RespType.INTEGER:
            return self.int_value
        # Try to parse string as int
        if self.is_string():
            try:
                return Int64(Int(self.string_value))
            except:
                return 0
        return 0

    fn as_bool(self) -> Bool:
        """Get value as boolean (for OK responses)."""
        if self.is_null:
            return False
        if self.is_string():
            return self.string_value == "OK" or self.string_value == "1"
        if self.is_integer():
            return self.int_value != 0
        return False

    fn __str__(self) -> String:
        """String representation for debugging."""
        if self.is_null:
            return "(nil)"
        if self.resp_type == RespType.ERROR:
            return "(error) " + self.string_value
        if self.resp_type == RespType.SIMPLE_STRING:
            return self.string_value
        if self.resp_type == RespType.BULK_STRING:
            return "\"" + self.string_value + "\""
        if self.resp_type == RespType.INTEGER:
            return "(integer) " + str(self.int_value)
        if self.resp_type == RespType.ARRAY:
            var result = String("(array)")
            for i in range(len(self.array_value)):
                result += "\n  " + str(i + 1) + ") " + str(self.array_value[i])
            return result
        return "(unknown)"


# =============================================================================
# RESP Encoder
# =============================================================================

struct RespEncoder:
    """
    Encodes commands and values into RESP format.

    Example:
        var encoder = RespEncoder()
        var cmd = encoder.encode_command("SET", List("key", "value"))
        # Returns: *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
    """

    fn __init__(out self):
        pass

    fn encode_bulk_string(self, value: String) -> String:
        """Encode a bulk string: $<length>\r\n<data>\r\n"""
        return BULK_STRING_PREFIX + str(len(value)) + CRLF + value + CRLF

    fn encode_array_header(self, count: Int) -> String:
        """Encode array header: *<count>\r\n"""
        return ARRAY_PREFIX + str(count) + CRLF

    fn encode_command(self, command: String) -> String:
        """Encode a single-word command (e.g., PING)."""
        return self.encode_array_header(1) + self.encode_bulk_string(command)

    fn encode_command(self, command: String, arg1: String) -> String:
        """Encode command with one argument."""
        return (
            self.encode_array_header(2) +
            self.encode_bulk_string(command) +
            self.encode_bulk_string(arg1)
        )

    fn encode_command(self, command: String, arg1: String, arg2: String) -> String:
        """Encode command with two arguments."""
        return (
            self.encode_array_header(3) +
            self.encode_bulk_string(command) +
            self.encode_bulk_string(arg1) +
            self.encode_bulk_string(arg2)
        )

    fn encode_command(self, command: String, arg1: String, arg2: String, arg3: String) -> String:
        """Encode command with three arguments."""
        return (
            self.encode_array_header(4) +
            self.encode_bulk_string(command) +
            self.encode_bulk_string(arg1) +
            self.encode_bulk_string(arg2) +
            self.encode_bulk_string(arg3)
        )

    fn encode_command_list(self, args: List[String]) -> String:
        """Encode command from list of strings."""
        var result = self.encode_array_header(len(args))
        for i in range(len(args)):
            result += self.encode_bulk_string(args[i])
        return result

    fn encode_integer(self, value: Int64) -> String:
        """Encode an integer: :<value>\r\n"""
        return INTEGER_PREFIX + str(value) + CRLF

    fn encode_simple_string(self, value: String) -> String:
        """Encode a simple string: +<value>\r\n"""
        return SIMPLE_STRING_PREFIX + value + CRLF

    fn encode_error(self, message: String) -> String:
        """Encode an error: -<message>\r\n"""
        return ERROR_PREFIX + message + CRLF

    fn encode_null(self) -> String:
        """Encode null bulk string: $-1\r\n"""
        return BULK_STRING_PREFIX + "-1" + CRLF


# =============================================================================
# RESP Decoder
# =============================================================================

struct RespDecoder:
    """
    Decodes RESP protocol responses from Redis.

    Example:
        var decoder = RespDecoder()
        var value = decoder.decode("+OK\r\n")
        # Returns: RespValue with string_value = "OK"
    """
    var buffer: String
    var pos: Int

    fn __init__(out self):
        self.buffer = ""
        self.pos = 0

    fn decode(inout self, data: String) -> RespValue:
        """Decode a RESP response."""
        self.buffer = data
        self.pos = 0
        return self._parse_value()

    fn _parse_value(inout self) -> RespValue:
        """Parse the next value from buffer."""
        if self.pos >= len(self.buffer):
            return RespValue.null()

        var prefix = self.buffer[self.pos]
        self.pos += 1

        if prefix == "+":
            return self._parse_simple_string()
        elif prefix == "-":
            return self._parse_error()
        elif prefix == ":":
            return self._parse_integer()
        elif prefix == "$":
            return self._parse_bulk_string()
        elif prefix == "*":
            return self._parse_array()
        else:
            return RespValue.error("Unknown RESP type: " + prefix)

    fn _parse_simple_string(inout self) -> RespValue:
        """Parse simple string until CRLF."""
        var line = self._read_line()
        return RespValue.simple_string(line)

    fn _parse_error(inout self) -> RespValue:
        """Parse error message until CRLF."""
        var line = self._read_line()
        return RespValue.error(line)

    fn _parse_integer(inout self) -> RespValue:
        """Parse integer value."""
        var line = self._read_line()
        try:
            var value = Int64(Int(line))
            return RespValue(value)
        except:
            return RespValue.error("Invalid integer: " + line)

    fn _parse_bulk_string(inout self) -> RespValue:
        """Parse bulk string with length prefix."""
        var length_str = self._read_line()
        try:
            var length = Int(length_str)

            # Null bulk string
            if length < 0:
                return RespValue.null()

            # Read exactly length bytes
            var value = self._read_bytes(length)

            # Skip trailing CRLF
            self.pos += 2

            return RespValue(value)
        except:
            return RespValue.error("Invalid bulk string length: " + length_str)

    fn _parse_array(inout self) -> RespValue:
        """Parse array of values."""
        var count_str = self._read_line()
        try:
            var count = Int(count_str)

            # Null array
            if count < 0:
                return RespValue.null()

            var values = List[RespValue]()
            for _ in range(count):
                values.append(self._parse_value())

            return RespValue.array(values)
        except:
            return RespValue.error("Invalid array count: " + count_str)

    fn _read_line(inout self) -> String:
        """Read until CRLF and return content (excluding CRLF)."""
        var start = self.pos
        while self.pos < len(self.buffer) - 1:
            if self.buffer[self.pos] == "\r" and self.buffer[self.pos + 1] == "\n":
                var line = self.buffer[start:self.pos]
                self.pos += 2  # Skip CRLF
                return line
            self.pos += 1
        return self.buffer[start:]

    fn _read_bytes(inout self, count: Int) -> String:
        """Read exactly count bytes from buffer."""
        var end = self.pos + count
        if end > len(self.buffer):
            end = len(self.buffer)
        var result = self.buffer[self.pos:end]
        self.pos = end
        return result


# =============================================================================
# Utility Functions
# =============================================================================

fn encode_command(command: String) -> String:
    """Convenience function to encode a command."""
    var encoder = RespEncoder()
    return encoder.encode_command(command)


fn encode_command(command: String, arg1: String) -> String:
    """Convenience function to encode a command with one argument."""
    var encoder = RespEncoder()
    return encoder.encode_command(command, arg1)


fn encode_command(command: String, arg1: String, arg2: String) -> String:
    """Convenience function to encode a command with two arguments."""
    var encoder = RespEncoder()
    return encoder.encode_command(command, arg1, arg2)


fn decode_response(data: String) -> RespValue:
    """Convenience function to decode a RESP response."""
    var decoder = RespDecoder()
    return decoder.decode(data)
