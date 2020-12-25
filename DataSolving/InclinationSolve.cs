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
    class InclinationConfig
    {
        public string SensorId { get; set; }
        public double InitX { get; set; }
        public double InitY { get; set; }
        public string TimeStamp { get; set; }

    }
    class InclinationSolve:DataSolve
    {
        private System.Timers.Timer timer;
        private BackgroundWorker backgroundWorker;
        private Dictionary<string, InclinationConfig> list;
        private Dictionary<string, string> stamp;
        private int redisDbIndex;
        private ConcurrentQueue<RabbitMsg> dataQueue;
        public InclinationSolve(Dictionary<string, InclinationConfig> keys, int period, ConnectionMultiplexer redis, int redisIndex, TextBox log, ConcurrentQueue<RabbitMsg> queue) : base(redis, log)
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
            foreach(RedisValue rv in vals)
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
                        ACA826T_Data dv = JsonConvert.DeserializeObject<ACA826T_Data>((string)rv);

                        string key = dv.SensorId;

                        InclinationConfig ptv = list[key];
                        if (ptv.TimeStamp != dv.TimeStamp)
                        {
                            ptv.TimeStamp = dv.TimeStamp;
                            ptv.SensorId = dv.SensorId;

                            Inclination_Data data = new Inclination_Data();
                            data.SensorId = dv.SensorId;
                            data.TimeStamp = dv.TimeStamp;
                            data.X = dv.X;
                            data.Y = dv.Y;
                            data.DeltaX = Math.Round(dv.X-ptv.InitX,3);
                            data.DeltaY = Math.Round(dv.Y-ptv.InitY,3);

                            string mq_string = JsonConvert.SerializeObject(data);
                            //mq_string to mq
                            RabbitMsg msg = new RabbitMsg();
                            msg.RouteKey = ptv.SensorId;
                            msg.Body = mq_string;
                            dataQueue.Enqueue(msg);

                            string redisKey = ptv.SensorId + "-001";
                            DataValue temp = new DataValue();
                            temp.SensorId = ptv.SensorId;
                            temp.TimeStamp = ptv.TimeStamp;
                            temp.ValueType = "001";
                            temp.Value = dv.X;
                            string result = JsonConvert.SerializeObject(temp);
                            pair[redisKey] = result;

                            redisKey = ptv.SensorId + "-002";
                            temp.ValueType = "002";
                            temp.Value = dv.Y;
                            result = JsonConvert.SerializeObject(temp);
                            pair[redisKey] = result;

                            redisKey = ptv.SensorId + "-018";
                            temp.ValueType = "018";
                            temp.Value = data.DeltaX;
                            result = JsonConvert.SerializeObject(temp);
                            pair[redisKey] = result;

                            redisKey = ptv.SensorId + "-019";
                            temp.ValueType = "019";
                            temp.Value = data.DeltaY;
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
