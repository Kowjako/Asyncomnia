using Asyncomnia.Messaging.Internal;
using Asyncomnia.Messaging.Tests.TestDoubles;

namespace Asyncomnia.Messaging.Tests.Internal;

public sealed class ClrMessageTypeResolverTests
{
    [Fact]
    public void GetContractType_ReturnsFullTypeNameAndSimpleAssemblyName()
    {
        var contractType = ClrMessageTypeResolver.GetContractType(
            typeof(TestEvent));

        Assert.Equal(
            "Asyncomnia.Messaging.Tests.TestDoubles.TestEvent, " +
            "Asyncomnia.Messaging.Tests",
            contractType);
        Assert.DoesNotContain("Version=", contractType);
        Assert.DoesNotContain("Culture=", contractType);
        Assert.DoesNotContain("PublicKeyToken=", contractType);
    }

    [Fact]
    public void Resolve_ForGeneratedContractType_ReturnsOriginalType()
    {
        var contractType = ClrMessageTypeResolver.GetContractType(
            typeof(TestCommand));

        var resolvedType = ClrMessageTypeResolver.Resolve(contractType);

        Assert.Equal(typeof(TestCommand), resolvedType);
    }

    [Fact]
    public void GetContractType_WhenMessageTypeIsNull_Throws()
    {
        Assert.Throws<ArgumentNullException>(
            () => ClrMessageTypeResolver.GetContractType(null!));
    }

    [Fact]
    public void GetContractType_WhenTypeDoesNotHaveFullName_Throws()
    {
        var genericParameter = typeof(GenericType<>)
            .GetGenericArguments()[0];

        var exception = Assert.Throws<InvalidOperationException>(
            () => ClrMessageTypeResolver.GetContractType(
                genericParameter));

        Assert.Contains("does not have a full name", exception.Message);
    }

    [Fact]
    public void Resolve_WhenContractTypeIsNull_Throws()
    {
        Assert.Throws<ArgumentNullException>(
            () => ClrMessageTypeResolver.Resolve(null!));
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void Resolve_WhenContractTypeIsEmpty_Throws(
        string contractType)
    {
        Assert.Throws<ArgumentException>(
            () => ClrMessageTypeResolver.Resolve(contractType));
    }

    [Fact]
    public void Resolve_WhenContractTypeCannotBeResolved_Throws()
    {
        const string contractType =
            "Missing.Namespace.Message, Missing.Assembly";

        var exception = Assert.Throws<InvalidOperationException>(
            () => ClrMessageTypeResolver.Resolve(contractType));

        Assert.Contains(contractType, exception.Message);
    }

    private sealed class GenericType<T>
    {
    }
}
