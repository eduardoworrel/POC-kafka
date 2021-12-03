using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using Serilog;
using Serilog.Sinks.SystemConsole.Themes;

namespace EnvioKafka
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var logger = new LoggerConfiguration()
                .WriteTo.Console(theme: AnsiConsoleTheme.Literate)
                .CreateLogger();
            logger.Information("Testando o envio de mensagens com Kafka");

            if (args.Length < 2)
            {
                logger.Error(
                    "Informe ao menos 2 parâmetros: " +
                    "no primeiro o IP/porta para testes com o Kafka, " +
                    "no segundo o Topic que receberá a mensagem");
                return;
            }

            string bootstrapServers = args[0];
            string nomeTopic = args[1];

            logger.Information($"BootstrapServers = {bootstrapServers}");
            logger.Information($"Topic = {nomeTopic}");

            try
            {
                var config = new ProducerConfig
                {
                    BootstrapServers = bootstrapServers
                };

                using (var producer = new ProducerBuilder<Null, string>(config).Build())
                {
                    var i = 0;
                    while(i < 300)
                    {
                        var result = await producer.ProduceAsync(
                            nomeTopic,
                            new () { Value = "int: "+i });

                        logger.Information(
                            $"Mensagem: {i} | " +
                            $"Status: { result.Status.ToString()}");
                            i++;
                    }
                }

                logger.Information("Concluído o envio de mensagens");
            }
            catch (Exception ex)
            {
                logger.Error($"Exceção: {ex.GetType().FullName} | " +
                             $"Mensagem: {ex.Message}");
            }
        }
    }
}