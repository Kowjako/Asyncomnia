using Asyncomnia.Messaging.Reliability.SqlServer.EFCore.Infrastructure;
using Asyncomnia.Messaging.Reliability.SqlServer.EFCore.Tests.TestDoubles;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;

namespace Asyncomnia.Messaging.Reliability.SqlServer.EFCore.Tests.Infrastructure;

public sealed class EntityFrameworkCoreSqlServerConnectionFactoryTests
{
    [Fact]
    public void Constructor_WhenDbContextIsNull_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new EntityFrameworkCoreSqlServerConnectionFactory<TestDbContext>(null!));
    }

    [Fact]
    public async Task OpenConnectionAsync_WhenDbContextHasNoRelationalConnectionString_ThrowsInvalidOperationException()
    {
        await using var connection = new SqlConnection();
        var options = new DbContextOptionsBuilder<TestDbContext>()
            .UseSqlServer(connection)
            .Options;
        await using var dbContext = new TestDbContext(options);
        var factory = new EntityFrameworkCoreSqlServerConnectionFactory<TestDbContext>(dbContext);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await factory.OpenConnectionAsync());

        Assert.Contains(typeof(TestDbContext).FullName!, exception.Message);
        Assert.Contains("does not contain a SQL Server connection string", exception.Message);
    }

    [Fact]
    public async Task OpenConnectionAsync_WhenCancellationIsRequested_ThrowsOperationCanceledException()
    {
        var options = new DbContextOptionsBuilder<TestDbContext>()
            .UseSqlServer(
                "Server=localhost;Database=AsyncomniaTests;Integrated Security=true;TrustServerCertificate=true")
            .Options;
        await using var dbContext = new TestDbContext(options);
        var factory = new EntityFrameworkCoreSqlServerConnectionFactory<TestDbContext>(dbContext);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await factory.OpenConnectionAsync(cancellation.Token));
    }
}
