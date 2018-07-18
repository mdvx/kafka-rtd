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

        static Random random = new Random();

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
                    int l = 0;

                    int r = 200;
                    var padding = new String('x', r);

                    while (true)
                    {
                        l++;

                        var topic = $"JSON_0";
                        var str = $"{{ \"topic\": \"{topic}\", \"FIELD\": {l}, \"len\": {r}, \"padding\": \"{padding}\"}}";

                        // Calling .Result on the asynchronous produce request below causes it to
                        // block until it completes. Generally, you should avoid producing
                        // synchronously because this has a huge impact on throughput. 
                        var dr = producer.ProduceAsync(topic, null, str);

                        if (l % 1000 == 0)
                        {
                            producer.Flush(TimeSpan.FromSeconds(10));
                            Logger.Info("sent " + str.Substring(0, Math.Min(75, str.Length)));

                            var d = random.NextDouble();
                            var e = random.Next(5);
                            r = (int)(d * Math.Pow(10, e)) + 1;  // r should fall between 0 and 4*100,000

                            padding = new String('x', r);
                        }
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
