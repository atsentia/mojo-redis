"""
Redis Client Tests

Tests for RESP protocol encoding/decoding and client functionality.
Note: Integration tests require a running Redis server.
"""

from mojo_redis import (
    RespType, RespValue, RespEncoder, RespDecoder,
    encode_command, decode_response,
    RedisConfig, RedisClient, Pipeline,
    CRLF,
)


# =============================================================================
# RESP Protocol Tests
# =============================================================================

fn test_resp_encode_bulk_string() raises:
    """Test encoding bulk strings."""
    var encoder = RespEncoder()

    var result = encoder.encode_bulk_string("hello")
    var expected = "$5\r\nhello\r\n"

    if result != expected:
        raise Error("Bulk string encoding failed: got '" + result + "', expected '" + expected + "'")

    print("  RESP encode bulk string")


fn test_resp_encode_array() raises:
    """Test encoding arrays."""
    var encoder = RespEncoder()

    var header = encoder.encode_array_header(3)
    if header != "*3\r\n":
        raise Error("Array header encoding failed")

    print("  RESP encode array header")


fn test_resp_encode_command() raises:
    """Test encoding commands."""
    var encoder = RespEncoder()

    # Single command
    var ping = encoder.encode_command("PING")
    if ping != "*1\r\n$4\r\nPING\r\n":
        raise Error("PING encoding failed: " + ping)

    # Command with argument
    var get = encoder.encode_command("GET", "mykey")
    if get != "*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n":
        raise Error("GET encoding failed: " + get)

    # Command with two arguments
    var set_cmd = encoder.encode_command("SET", "key", "value")
    if set_cmd != "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n":
        raise Error("SET encoding failed: " + set_cmd)

    print("  RESP encode commands")


fn test_resp_encode_command_list() raises:
    """Test encoding command from list."""
    var encoder = RespEncoder()

    var args = List[String]()
    args.append("MSET")
    args.append("key1")
    args.append("val1")
    args.append("key2")
    args.append("val2")

    var result = encoder.encode_command_list(args)
    var expected = "*5\r\n$4\r\nMSET\r\n$4\r\nkey1\r\n$4\r\nval1\r\n$4\r\nkey2\r\n$4\r\nval2\r\n"

    if result != expected:
        raise Error("MSET encoding failed")

    print("  RESP encode command list")


fn test_resp_decode_simple_string() raises:
    """Test decoding simple strings."""
    var decoder = RespDecoder()

    var result = decoder.decode("+OK\r\n")

    if result.resp_type != RespType.SIMPLE_STRING:
        raise Error("Should be simple string type")

    if result.string_value != "OK":
        raise Error("Simple string decode failed: got '" + result.string_value + "'")

    print("  RESP decode simple string")


fn test_resp_decode_error() raises:
    """Test decoding errors."""
    var decoder = RespDecoder()

    var result = decoder.decode("-ERR unknown command\r\n")

    if result.resp_type != RespType.ERROR:
        raise Error("Should be error type")

    if not result.is_error():
        raise Error("is_error() should return True")

    if result.string_value != "ERR unknown command":
        raise Error("Error decode failed: got '" + result.string_value + "'")

    print("  RESP decode error")


fn test_resp_decode_integer() raises:
    """Test decoding integers."""
    var decoder = RespDecoder()

    var result = decoder.decode(":1000\r\n")

    if result.resp_type != RespType.INTEGER:
        raise Error("Should be integer type")

    if result.int_value != 1000:
        raise Error("Integer decode failed: got " + str(result.int_value))

    # Negative integer
    var neg = decoder.decode(":-50\r\n")
    if neg.int_value != -50:
        raise Error("Negative integer decode failed")

    print("  RESP decode integer")


fn test_resp_decode_bulk_string() raises:
    """Test decoding bulk strings."""
    var decoder = RespDecoder()

    var result = decoder.decode("$6\r\nfoobar\r\n")

    if result.resp_type != RespType.BULK_STRING:
        raise Error("Should be bulk string type")

    if result.string_value != "foobar":
        raise Error("Bulk string decode failed: got '" + result.string_value + "'")

    print("  RESP decode bulk string")


fn test_resp_decode_null_bulk_string() raises:
    """Test decoding null bulk strings."""
    var decoder = RespDecoder()

    var result = decoder.decode("$-1\r\n")

    if not result.is_null:
        raise Error("Should be null")

    if result.resp_type != RespType.NULL:
        raise Error("Should be null type")

    print("  RESP decode null bulk string")


fn test_resp_decode_array() raises:
    """Test decoding arrays."""
    var decoder = RespDecoder()

    # Simple array of bulk strings
    var result = decoder.decode("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")

    if result.resp_type != RespType.ARRAY:
        raise Error("Should be array type")

    if len(result.array_value) != 2:
        raise Error("Array should have 2 elements, got " + str(len(result.array_value)))

    if result.array_value[0].string_value != "foo":
        raise Error("First element should be 'foo'")

    if result.array_value[1].string_value != "bar":
        raise Error("Second element should be 'bar'")

    print("  RESP decode array")


fn test_resp_decode_null_array() raises:
    """Test decoding null arrays."""
    var decoder = RespDecoder()

    var result = decoder.decode("*-1\r\n")

    if not result.is_null:
        raise Error("Should be null array")

    print("  RESP decode null array")


fn test_resp_decode_mixed_array() raises:
    """Test decoding arrays with mixed types."""
    var decoder = RespDecoder()

    # Array with integer and string
    var result = decoder.decode("*2\r\n:42\r\n$5\r\nhello\r\n")

    if len(result.array_value) != 2:
        raise Error("Should have 2 elements")

    if result.array_value[0].int_value != 42:
        raise Error("First element should be 42")

    if result.array_value[1].string_value != "hello":
        raise Error("Second element should be 'hello'")

    print("  RESP decode mixed array")


fn test_resp_decode_empty_array() raises:
    """Test decoding empty arrays."""
    var decoder = RespDecoder()

    var result = decoder.decode("*0\r\n")

    if result.resp_type != RespType.ARRAY:
        raise Error("Should be array type")

    if len(result.array_value) != 0:
        raise Error("Should be empty array")

    print("  RESP decode empty array")


# =============================================================================
# RespValue Tests
# =============================================================================

fn test_resp_value_constructors() raises:
    """Test RespValue constructors."""
    # Null value
    var null_val = RespValue()
    if not null_val.is_null:
        raise Error("Default constructor should create null")

    # String value
    var str_val = RespValue("hello")
    if str_val.string_value != "hello":
        raise Error("String constructor failed")

    # Integer value
    var int_val = RespValue(Int64(42))
    if int_val.int_value != 42:
        raise Error("Integer constructor failed")

    # Static constructors
    var simple = RespValue.simple_string("OK")
    if simple.resp_type != RespType.SIMPLE_STRING:
        raise Error("simple_string should set correct type")

    var error = RespValue.error("test error")
    if not error.is_error():
        raise Error("error should set error type")

    print("  RespValue constructors")


fn test_resp_value_conversions() raises:
    """Test RespValue type conversions."""
    # as_string
    var str_val = RespValue("hello")
    if str_val.as_string() != "hello":
        raise Error("as_string failed for string")

    var int_val = RespValue(Int64(42))
    if int_val.as_string() != "42":
        raise Error("as_string failed for integer")

    # as_int
    if int_val.as_int() != 42:
        raise Error("as_int failed for integer")

    var str_num = RespValue("123")
    if str_num.as_int() != 123:
        raise Error("as_int failed for string number")

    # as_bool
    var ok_val = RespValue.simple_string("OK")
    if not ok_val.as_bool():
        raise Error("as_bool failed for OK")

    var one_val = RespValue(Int64(1))
    if not one_val.as_bool():
        raise Error("as_bool failed for 1")

    var zero_val = RespValue(Int64(0))
    if zero_val.as_bool():
        raise Error("as_bool should be False for 0")

    print("  RespValue conversions")


# =============================================================================
# Client Configuration Tests
# =============================================================================

fn test_redis_config_defaults() raises:
    """Test RedisConfig default values."""
    var config = RedisConfig()

    if config.host != "127.0.0.1":
        raise Error("Default host should be 127.0.0.1")

    if config.port != 6379:
        raise Error("Default port should be 6379")

    if config.db != 0:
        raise Error("Default db should be 0")

    if config.timeout_seconds != 30:
        raise Error("Default timeout should be 30")

    print("  RedisConfig defaults")


fn test_redis_config_custom() raises:
    """Test RedisConfig custom values."""
    var config = RedisConfig("redis.example.com", 6380)

    if config.host != "redis.example.com":
        raise Error("Custom host not set")

    if config.port != 6380:
        raise Error("Custom port not set")

    var full_config = RedisConfig("host", 1234, "secret", 5)
    if full_config.password != "secret":
        raise Error("Password not set")

    if full_config.db != 5:
        raise Error("Database not set")

    print("  RedisConfig custom")


# =============================================================================
# Client Creation Tests (No Connection Required)
# =============================================================================

fn test_redis_client_creation() raises:
    """Test RedisClient creation."""
    var client = RedisClient()

    if client.is_connected:
        raise Error("New client should not be connected")

    if client.socket_fd != -1:
        raise Error("Socket fd should be -1")

    print("  RedisClient creation")


fn test_redis_client_with_config() raises:
    """Test RedisClient with custom config."""
    var config = RedisConfig("localhost", 6380)
    var client = RedisClient(config)

    if client.config.port != 6380:
        raise Error("Config not applied to client")

    print("  RedisClient with config")


# =============================================================================
# Utility Function Tests
# =============================================================================

fn test_encode_command_utility() raises:
    """Test encode_command utility functions."""
    # No args
    var ping = encode_command("PING")
    if ping != "*1\r\n$4\r\nPING\r\n":
        raise Error("encode_command(PING) failed")

    # One arg
    var get = encode_command("GET", "key")
    if get != "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n":
        raise Error("encode_command(GET, key) failed")

    # Two args
    var set_cmd = encode_command("SET", "k", "v")
    if set_cmd != "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n":
        raise Error("encode_command(SET, k, v) failed")

    print("  encode_command utility")


fn test_decode_response_utility() raises:
    """Test decode_response utility function."""
    var result = decode_response("+PONG\r\n")

    if result.string_value != "PONG":
        raise Error("decode_response failed")

    print("  decode_response utility")


# =============================================================================
# Main Test Runner
# =============================================================================

fn run_protocol_tests() raises:
    """Run all RESP protocol tests."""
    print("\nRESP Protocol Tests:")

    test_resp_encode_bulk_string()
    test_resp_encode_array()
    test_resp_encode_command()
    test_resp_encode_command_list()
    test_resp_decode_simple_string()
    test_resp_decode_error()
    test_resp_decode_integer()
    test_resp_decode_bulk_string()
    test_resp_decode_null_bulk_string()
    test_resp_decode_array()
    test_resp_decode_null_array()
    test_resp_decode_mixed_array()
    test_resp_decode_empty_array()

    print("  All RESP protocol tests passed!")


fn run_value_tests() raises:
    """Run all RespValue tests."""
    print("\nRespValue Tests:")

    test_resp_value_constructors()
    test_resp_value_conversions()

    print("  All RespValue tests passed!")


fn run_config_tests() raises:
    """Run all configuration tests."""
    print("\nConfiguration Tests:")

    test_redis_config_defaults()
    test_redis_config_custom()

    print("  All configuration tests passed!")


fn run_client_tests() raises:
    """Run client creation tests (no connection)."""
    print("\nClient Creation Tests:")

    test_redis_client_creation()
    test_redis_client_with_config()

    print("  All client creation tests passed!")


fn run_utility_tests() raises:
    """Run utility function tests."""
    print("\nUtility Function Tests:")

    test_encode_command_utility()
    test_decode_response_utility()

    print("  All utility tests passed!")


fn main() raises:
    print("=" * 60)
    print("mojo-redis Test Suite")
    print("=" * 60)

    run_protocol_tests()
    run_value_tests()
    run_config_tests()
    run_client_tests()
    run_utility_tests()

    print("\n" + "=" * 60)
    print("All tests passed!")
    print("=" * 60)

    print("\nNote: Integration tests require a running Redis server.")
    print("To test with Redis, uncomment the integration tests below.")

    # Uncomment to run integration tests (requires Redis server):
    # run_integration_tests()


# =============================================================================
# Integration Tests (Require Redis Server)
# =============================================================================

fn run_integration_tests() raises:
    """
    Run integration tests against a real Redis server.

    Uncomment the call in main() to run these tests.
    Requires Redis running on localhost:6379.
    """
    print("\n" + "=" * 60)
    print("Integration Tests (requires Redis on localhost:6379)")
    print("=" * 60)

    var client = RedisClient("127.0.0.1", 6379)

    try:
        client.connect()
        print("  Connected to Redis")
    except e:
        print("  Skipping integration tests - Redis not available")
        print("  Error: " + str(e))
        return

    try:
        # Ping test
        if client.ping():
            print("  PING/PONG successful")
        else:
            raise Error("PING failed")

        # String operations
        var set_result = client.set("mojo_test_key", "mojo_test_value")
        if set_result.as_string() != "OK":
            raise Error("SET failed")
        print("  SET successful")

        var get_result = client.get("mojo_test_key")
        if get_result.as_string() != "mojo_test_value":
            raise Error("GET failed: got '" + get_result.as_string() + "'")
        print("  GET successful")

        # Counter operations
        _ = client.set("mojo_test_counter", "0")
        var incr1 = client.incr("mojo_test_counter")
        if incr1.as_int() != 1:
            raise Error("INCR failed")
        print("  INCR successful")

        var incrby = client.incrby("mojo_test_counter", 10)
        if incrby.as_int() != 11:
            raise Error("INCRBY failed")
        print("  INCRBY successful")

        var decr = client.decr("mojo_test_counter")
        if decr.as_int() != 10:
            raise Error("DECR failed")
        print("  DECR successful")

        # Existence check
        if not client.exists("mojo_test_key"):
            raise Error("EXISTS failed for existing key")
        print("  EXISTS successful")

        # Hash operations
        _ = client.hset("mojo_test_hash", "field1", "value1")
        var hget_result = client.hget("mojo_test_hash", "field1")
        if hget_result.as_string() != "value1":
            raise Error("HGET failed")
        print("  HSET/HGET successful")

        # List operations
        _ = client.delete("mojo_test_list")  # Clean up first
        var lpush1 = client.lpush("mojo_test_list", "item1")
        var lpush2 = client.lpush("mojo_test_list", "item2")
        if lpush2.as_int() != 2:
            raise Error("LPUSH failed")
        print("  LPUSH successful")

        var lpop_result = client.lpop("mojo_test_list")
        if lpop_result.as_string() != "item2":
            raise Error("LPOP failed: got '" + lpop_result.as_string() + "'")
        print("  LPOP successful")

        # TTL operations
        if client.expire("mojo_test_key", 60):
            var ttl_val = client.ttl("mojo_test_key")
            if ttl_val < 0 or ttl_val > 60:
                raise Error("TTL unexpected value: " + str(ttl_val))
            print("  EXPIRE/TTL successful")
        else:
            print("  EXPIRE returned false (key may not exist)")

        # Cleanup
        _ = client.delete("mojo_test_key")
        _ = client.delete("mojo_test_counter")
        _ = client.delete("mojo_test_hash")
        _ = client.delete("mojo_test_list")
        print("  Cleanup successful")

        print("\n  All integration tests passed!")

    except e:
        print("  Integration test failed: " + str(e))
    finally:
        client.close()
        print("  Disconnected from Redis")
