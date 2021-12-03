using System;
using System.Text;
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
            logger.Information("Rodada Tecnica com Kafka");

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
                            new () { Value = "String App: " + RandomString(5) });

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
    
        private static Random random = new Random((int)DateTime.Now.Ticks);
        
        private static string RandomString(int size)
        {
        StringBuilder builder = new StringBuilder();
        char ch;
        for (int i = 0; i < size; i++)
        {
            ch = Convert.ToChar(Convert.ToInt32(Math.Floor(26 * random.NextDouble() + 65)));                 
            builder.Append(ch);
        }

        return builder.ToString();
        }
    }
}