using Newtonsoft.Json;
using StackExchange.Redis;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Text;
using System.Timers;
using System.Windows.Forms;

namespace DataSolving
{
    class TemperatureHumidityConfig
    {
        public string SensorId { get; set; }
        public string Stamp { get; set; }
    }
    class TemperatureHumiditySolve : DataSolve
    {
        private System.Timers.Timer timer;
        private BackgroundWorker backgroundWorker;
        private Dictionary<string, TemperatureHumidityConfig> list;
        private Dictionary<string, string> stamp;
        private int redisDbIndex;
        private ConcurrentQueue<RabbitMsg> dataQueue;
        public TemperatureHumiditySolve(Dictionary<string, TemperatureHumidityConfig> keys, int period, ConnectionMultiplexer redis, int redisIndex, TextBox log, ConcurrentQueue<RabbitMsg> queue) : base(redis, log)
        {
            dataQueue = queue;
            list = keys;
            timer = new System.Timers.Timer(period * 1000);
            stamp = new Dictionary<string, string>();
            timer.Elapsed += Timer_Elapsed;
            redisDbIndex = redisIndex;
            backgroundWorker = new BackgroundWorker();
            backgroundWorker.WorkerSupportsCancellation = true;
            backgroundWorker.DoWork += BackgroundWorker_DoWork;
        }

        private void Timer_Elapsed(object sender, ElapsedEventArgs e)
        {
            if (!backgroundWorker.IsBusy)
            {
                backgroundWorker.RunWorkerAsync();
            }
        }

        public override void Start()
        {
            timer.Start();
            //backgroundWorker.RunWorkerAsync();
        }

        public override void Stop()
        {
            timer.Stop();
            backgroundWorker.CancelAsync();
        }

        private string GetDataValues()
        {
            IDatabase db = this.redis.GetDatabase(0);
            string[] ks = list.Keys.ToArray<string>();

            RedisKey[] keys = ks.Select(key => (RedisKey)key).ToArray();

            RedisValue[] vals = db.StringGet(keys);
            RedisValuesToDataValues(vals);
            return null;
        }

        private DataValue[] RedisValuesToDataValues(RedisValue[] vals)
        {
            List<DataValue> dv_list = new List<DataValue>();
            foreach (RedisValue rv in vals)
            {
                if (!rv.IsNull)
                {
                    DataValue dv = JsonConvert.DeserializeObject<DataValue>((string)rv);
                    dv_list.Add(dv);
                }
                else
                {
                    //error log
                }
            }
            return dv_list.ToArray();
        }

        private void BackgroundWorker_DoWork(object sender, DoWorkEventArgs e)
        {
            BackgroundWorker bgWorker = sender as BackgroundWorker;
            string stamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
            string str = "";
            str += stamp + " ";
            if (redis.IsConnected)
            {

            }
            else
            {
                str += "redis server is not connected";
                //lthis.AppendLog(str);
                return;
            }
            Dictionary<RedisKey, RedisValue> pair = new Dictionary<RedisKey, RedisValue>();
            try
            {
                IDatabase db_raw = this.redis.GetDatabase(0);
                IDatabase db_result = this.redis.GetDatabase(redisDbIndex);

                List<RedisKey> keyCollection = new List<RedisKey>();
                foreach (string k in list.Keys)
                {
                    keyCollection.Add(k);
                }

                RedisKey[] keys = keyCollection.ToArray();

                RedisValue[] vals = db_raw.StringGet(keys);

                foreach (RedisValue rv in vals)
                {
                    if (!rv.IsNull)
                    {
                        TemperatureHumidity_Data data = JsonConvert.DeserializeObject<TemperatureHumidity_Data>((string)rv);

                        string key = data.SensorId;

                        TemperatureHumidityConfig ptv = list[key];

                        if (ptv.Stamp != data.TimeStamp)
                        {
                            ptv.Stamp = data.TimeStamp;

                            string mq_string = JsonConvert.SerializeObject(data);
                            //mq_string to mq
                            RabbitMsg msg = new RabbitMsg();
                            msg.RouteKey = ptv.SensorId;
                            msg.Body = mq_string;
                            dataQueue.Enqueue(msg);

                            string redisKey = data.SensorId + "-005";
                            DataValue temp = new DataValue();
                            temp.SensorId = data.SensorId;
                            temp.TimeStamp = data.TimeStamp;
                            temp.ValueType = "005";
                            temp.Value = data.Temperature;
                            string result = JsonConvert.SerializeObject(temp);
                            pair[redisKey] = result;

                            redisKey = data.SensorId + "-006";
                            temp = new DataValue();
                            temp.SensorId = data.SensorId;
                            temp.TimeStamp = data.TimeStamp;
                            temp.ValueType = "006";
                            temp.Value = data.Humidity;
                            result = JsonConvert.SerializeObject(temp);
                            pair[redisKey] = result;
                        }
                    }
                }

                if (pair.Count > 0)
                {
                    db_result.StringSet(pair.ToArray());
                    pair.Clear();
                }
            }
            catch (Exception ex)
            {
                this.AppendLog(ex.Message);
                using (StreamWriter sw = new StreamWriter(@"ErrLog.txt", true))
                {
                    sw.WriteLine(stamp + " " + ex.Message + " \r\n" + ex.StackTrace.ToString());
                    sw.WriteLine("---------------------------------------------------------");
                    sw.Close();
                }
            }
        }
    }
}

