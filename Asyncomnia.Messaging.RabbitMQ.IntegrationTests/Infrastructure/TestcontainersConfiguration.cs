using System.Runtime.CompilerServices;

namespace Asyncomnia.Messaging.RabbitMQ.IntegrationTests.Infrastructure;

internal static class TestcontainersConfiguration
{
    [ModuleInitializer]
    internal static void Initialize()
    {
        if (!OperatingSystem.IsWindows())
        {
            return;
        }

        const string variableName = "DOCKER_API_VERSION";

        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(variableName)))
        {
            Environment.SetEnvironmentVariable(variableName, "1.43");
        }
    }
}
