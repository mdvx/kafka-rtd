using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using NLog;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace kafka_rtd
{
    [
        Guid("653ED879-32B3-469D-BE74-831652D78919"),
        // This is the string that names RTD server.
        // Users will use it from Excel: =RTD("kafka",, ....)
        ProgId("kafka")
    ]
    public class KafkaRtdServer : IRtdServer
    {
        private static readonly ILogger Logger = LogManager.GetCurrentClassLogger();

        IRtdUpdateEvent _callback;
        ILogger Log = LogManager.GetCurrentClassLogger();

        //DispatcherTimer _timer;
        SubscriptionManager _subMgr;
        bool _isExcelNotifiedOfUpdates = false;
        object _notifyLock = new object();
        object _syncLock = new object();
        int _lastRtdTopic = -1;
        //private Dictionary<Uri, BrokerRouter> _kafkaConnect = new Dictionary<Uri, BrokerRouter>();

        private const string LAST_RTD = "LAST_RTD";

        public KafkaRtdServer()
        {
        }
        // Excel calls this. It's an entry point. It passes us a callback
        // structure which we save for later.
        int IRtdServer.ServerStart (IRtdUpdateEvent callback)
        {
            _callback = callback;
            _subMgr = new SubscriptionManager(() => {
                try
                {
                    if (_callback != null)
                    {
                        if (!_isExcelNotifiedOfUpdates)
                        {
                            lock (_notifyLock)
                            {
                                _callback.UpdateNotify();
                                _isExcelNotifiedOfUpdates = true;
                            }
                        }
                    }
                } catch(Exception ex)  // HRESULT: 0x8001010A (RPC_E_SERVERCALL_RETRYLATER
                {
                    Console.WriteLine(ex.Message);
                }
            });
            return 1;
        }

        // Excel calls this when it wants to shut down RTD server.
        void IRtdServer.ServerTerminate ()
        {
            _callback = null;
        }
        // Excel calls this when it wants to make a new topic subscription.
        // topicId becomes the key representing the subscription.
        // String array contains any aux data user provides to RTD macro.
        object IRtdServer.ConnectData (int topicId,
                                       ref Array strings,
                                       ref bool newValues)
        {
            try
            {
                newValues = true;

                if (strings.Length == 1)
                {
                    string host = strings.GetValue(0).ToString().ToUpperInvariant();

                    switch (host)
                    {
                        case LAST_RTD:
                            _lastRtdTopic = topicId;
                            return DateTime.Now.ToLocalTime();
                    }
                    return "ERROR: Expected: LAST_RTD or host, topic, field";
                }
                else if (strings.Length >= 2)
                {
                    // Crappy COM-style arrays...
                    string host = strings.GetValue(0).ToString().ToUpperInvariant();
                    string topic = strings.GetValue(1).ToString();
                    string field = strings.Length > 2 ? strings.GetValue(2).ToString() : "";

                    return Subscribe(topicId, host, topic, field);
                }
                return "ERROR: Expected: LAST_RTD or host, topic, field";

            }
            catch (Exception ex)
            {
                Log.Error(ex);
                return ex.Message;
            }
        }

        private object Subscribe(int topicId, string host, string topic, string field)
        {
            try
            {
                if (String. IsNullOrEmpty(topic))
                    return "<topic required>";

                //Uri hostUri = new Uri(host);
                //if (!_kafkaConnect.TryGetValue(hostUri, out BrokerRouter router))
                //{
                //    var options = new KafkaOptions(hostUri);
                //    router = new BrokerRouter(options);
                //    _kafkaConnect[hostUri] = router;
                //}

                CancellationTokenSource cts = new CancellationTokenSource();
                Task.Run(() =>
                {
                    try
                    {
                        var conf = new Dictionary<string, object>
                        {
                          { "group.id", "test-consumer-group" },
                          { "bootstrap.servers", "localhost:9092" },
                          { "auto.commit.interval.ms", 5000 },
                          { "auto.offset.reset", "earliest" }
                        };

                        using (var consumer = new Consumer<Null, string>(conf, null, new StringDeserializer(Encoding.UTF8)))
                        {
                            consumer.OnMessage += (_, msg) => Console.WriteLine($"Read '{msg.Value}' from: {msg.TopicPartitionOffset}");
                            consumer.OnError += (_, error) => Console.WriteLine($"Error: {error}");
                            consumer.OnConsumeError += (_, msg) => Console.WriteLine($"Consume error ({msg.TopicPartitionOffset}): {msg.Error}");

                            consumer.Subscribe(topic);
                            while (!cts.Token.IsCancellationRequested)
                            {
                                consumer.Poll(TimeSpan.FromMilliseconds(100));
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Log.Error(ex);
                    }
                });
                return SubscriptionManager.UninitializedValue;


                //    if (_subMgr.Subscribe(topicId, hostUri, topic, field))
                //        return _subMgr.GetValue(topicId); // already subscribed 

                //    var rtdSubTopic = SubscriptionManager.FormatPath(host, topic);
                //    var consumer = new Consumer(new ConsumerOptions(topic, router));
                //    foreach (var message in consumer.Consume())
                //    {
                //        try
                //        {
                //            Console.WriteLine("Response: P{0},O{1} : {2}",
                //                message.Meta.PartitionId, message.Meta.Offset, message.Value);

                //            var str = message.Value.ToString();
                //            _subMgr.Set(rtdSubTopic, str);

                //            if (str.StartsWith("{"))
                //            {
                //                var jo = JsonConvert.DeserializeObject<Dictionary<String, object>>(str);

                //                lock (_syncLock)
                //                {
                //                    foreach (string field_in in jo.Keys)
                //                    {
                //                        var rtdTopicString = SubscriptionManager.FormatPath(host, topic, field_in);
                //                        object val = jo[field_in];

                //                        _subMgr.Set(rtdTopicString, val);
                //                    }
                //                }
                //            }
                //        }
                //        catch (Exception ex)
                //        {
                //            _subMgr.Set(rtdSubTopic, ex.Message);
                //        }
                //    }
            }
            catch (Exception ex)
            {
                Log.Error(ex);
                _subMgr.Set(topicId, ex.Message);
            }
            return _subMgr.GetValue(topicId);
        }
        // Excel calls this when it wants to cancel subscription.
        void IRtdServer.DisconnectData (int topicId)
        {
            _subMgr.Unsubscribe(topicId);
        }
        // Excel calls this every once in a while.
        int IRtdServer.Heartbeat ()
        {
            lock (_notifyLock)  
                _isExcelNotifiedOfUpdates = false;  // just in case it gets stuck

            return 1;
        }
        // Excel calls this to get changed values. 
        Array IRtdServer.RefreshData (ref int topicCount)
        {
            try
            {
                List<SubscriptionManager.UpdatedValue> updates;
                lock (_syncLock)
                {
                    updates = _subMgr.GetUpdatedValues();
                }
                topicCount = updates.Count + (_lastRtdTopic < 0 ? 0 : 1);

                object[,] data = new object[2, topicCount];
                int i = 0;
                if (_lastRtdTopic > 0)
                {
                    data[0, i] = _lastRtdTopic;
                    data[1, i] = DateTime.Now.ToLocalTime();
                    i++;
                }

                foreach (var info in updates)
                {
                    data[0, i] = info.TopicId;
                    data[1, i] = info.Value;

                    i++;
                }
                return data;
            } 
            finally
            {
                lock (_notifyLock)
                    _isExcelNotifiedOfUpdates = false;
            }
        }
    }
}
