using System;
using System.Text;
using System.Collections.Generic;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;

using NLog;
using System.Threading;

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

                var config = new Dictionary<string, object>()
                {
                    { "bootstrap.servers", "localhost:9092" }
                };

                if (args.Length >= 1)
                    config["bootstrap.servers"] = args[0];

                using (var producer = new Producer<Null, string>(config, null, new StringSerializer(Encoding.UTF8)))
                {
                    int l = 0;

                    int r = 200;
                    var padding = new String('x', r);

                    string[] str = new string[3];
                    string[] topic = { "JSON_0", "JSON_1", "JSON_2" };

                    for (int i = 0; i < str.Length; i++)
                        str[i] = $"{{ \"topic\": \"{topic[i]}\", \"FIELD\": {l}, \"len\": {r}, \"padding\": \"{padding}\"}}";

                    while (true)
                    {
                        l++;
                        for (int i = 0; i < str.Length; i++)
                        {
                            var dr = producer.ProduceAsync(topic[i], null, str[i]);
                        }

                        //producer.Flush(TimeSpan.FromSeconds(1));

                        if (l % 1000 == 0)
                        {
                            int res = producer.Flush(TimeSpan.FromSeconds(10));
                            Logger.Info($"sent {res}: " + str[0].Substring(0, Math.Min(65, str[0].Length)));

                            var d = random.NextDouble();
                            var e = random.Next(5);
                            r = (int)(d * Math.Pow(10, e)) + 1;  // r should fall between 0 and 4*100,000

                            padding = new String('x', r);

                            for (int i = 0; i < str.Length; i++)
                                str[i] = $"{{ \"topic\": \"{topic[i]}\", \"FIELD\": {l}, \"len\": {r}, \"padding\": \"{padding}\"}}";

                            Thread.Sleep(1000);
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
