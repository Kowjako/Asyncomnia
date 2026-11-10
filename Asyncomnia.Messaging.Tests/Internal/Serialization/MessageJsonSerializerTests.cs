using System.Text;
using System.Text.Json;
using Asyncomnia.Messaging.Internal.Serialization;
using Asyncomnia.Messaging.Tests.TestDoubles;

namespace Asyncomnia.Messaging.Tests.Internal.Serialization;

public sealed class MessageJsonSerializerTests
{
    private readonly MessageJsonSerializer _serializer = new();

    [Fact]
    public void SerializeToString_UsesRuntimeMessageTypeAndWebJsonDefaults()
    {
        IMessage message = new TestEvent("Acme");

        var messageJson = _serializer.SerializeToString(message);

        Assert.Equal("{\"value\":\"Acme\"}", messageJson);
    }

    [Fact]
    public void SerializeToUtf8Bytes_ProducesSameJsonAsStringSerialization()
    {
        var message = new TestCommand("run-job");

        var messageJson = _serializer.SerializeToString(message);
        var body = _serializer.SerializeToUtf8Bytes(message);

        Assert.Equal(messageJson, Encoding.UTF8.GetString(body));
    }

    [Fact]
    public void Deserialize_FromString_ReturnsTypedMessage()
    {
        var originalMessage = new TestEvent("Acme");
        var messageJson = _serializer.SerializeToString(originalMessage);

        var message = _serializer.Deserialize(
            messageJson,
            typeof(TestEvent));

        Assert.Equal(originalMessage, Assert.IsType<TestEvent>(message));
    }

    [Fact]
    public void Deserialize_FromUtf8Bytes_ReturnsTypedMessage()
    {
        var originalMessage = new TestCommand("run-job");
        var body = _serializer.SerializeToUtf8Bytes(originalMessage);

        var message = _serializer.Deserialize(
            body,
            typeof(TestCommand));

        Assert.Equal(
            originalMessage,
            Assert.IsType<TestCommand>(message));
    }

    [Fact]
    public void Serialize_WhenMessageIsNull_Throws()
    {
        Assert.Throws<ArgumentNullException>(
            () => _serializer.SerializeToString(null!));
        Assert.Throws<ArgumentNullException>(
            () => _serializer.SerializeToUtf8Bytes(null!));
    }

    [Fact]
    public void Deserialize_WhenInputIsEmpty_Throws()
    {
        Assert.Throws<ArgumentException>(
            () => _serializer.Deserialize(
                string.Empty,
                typeof(TestEvent)));
        Assert.Throws<ArgumentException>(
            () => _serializer.Deserialize(
                ReadOnlyMemory<byte>.Empty,
                typeof(TestEvent)));
    }

    [Fact]
    public void Deserialize_WhenMessageTypeDoesNotImplementIMessage_Throws()
    {
        var exception = Assert.Throws<InvalidOperationException>(
            () => _serializer.Deserialize(
                "\"not-a-message\"",
                typeof(string)));

        Assert.Contains(nameof(IMessage), exception.Message);
    }

    [Fact]
    public void Deserialize_WhenJsonRepresentsNull_Throws()
    {
        var exception = Assert.Throws<InvalidOperationException>(
            () => _serializer.Deserialize(
                "null",
                typeof(TestEvent)));

        Assert.Contains(typeof(TestEvent).ToString(), exception.Message);
    }

    [Fact]
    public void Deserialize_WhenJsonIsMalformed_PropagatesJsonException()
    {
        Assert.Throws<JsonException>(
            () => _serializer.Deserialize(
                "{not-json}",
                typeof(TestEvent)));
    }
}
