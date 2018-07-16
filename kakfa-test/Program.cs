using System;
using System.Text;
using System.Collections.Generic;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using NLog;

namespace kakfa_test
{
    class Program
    {
        private static readonly ILogger Logger = LogManager.GetCurrentClassLogger();

        static void Main(string[] args)
        {

            try
            {
                Logger.Info("Start Main");

                var config = new Dictionary<string, object>
                {
                    { "bootstrap.servers", "localhost:9092" }
                };

                using (var producer = new Producer<Null, string>(config, null, new StringSerializer(Encoding.UTF8)))
                {
                    while (true)
                    {
                        var dr = producer.ProduceAsync("JSON_1", null, "test message text").Result;
                        Logger.Info($"Delivered '{dr.Value}' to: {dr.TopicPartitionOffset}");

                        //var dr = producer.ProduceAsync("JSON_2", null, "test message text").Result;
                        //var dr = producer.ProduceAsync("JSON_3", null, "test message text").Result;
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.Fatal(ex);
            }
        }
    }
}
