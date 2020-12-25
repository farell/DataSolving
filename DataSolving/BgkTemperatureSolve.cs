using Newtonsoft.Json;
using StackExchange.Redis;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;
using System.Windows.Forms;

namespace DataSolving
{
    public class BgkTemperatureConfig
    {
        public string SensorId;
        public double temperature;
        public string Stamp;
        public BgkTemperatureConfig(string sensorId)
        {
            this.SensorId = sensorId;
            this.temperature = 0;
            this.Stamp = "";
        }
    }
    
    class BgkTemperatureSolve : DataSolve
    {
        private System.Timers.Timer timer;
        private BackgroundWorker backgroundWorker;
        private Dictionary<string, BgkTemperatureConfig> list;
        private Dictionary<string, string> stamp;
        private int redisDbIndex;
        private ConcurrentQueue<RabbitMsg> dataQueue;
        public BgkTemperatureSolve(Dictionary<string, BgkTemperatureConfig> keys, int period, ConnectionMultiplexer redis, int redisIndex, TextBox log, ConcurrentQueue<RabbitMsg> queue) : base(redis, log)
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

        private double ResistanceToTemperature(double resistance)
        {
            double a = 1.4051e-3;
            double b = 2.369e-4;
            double c = 1.019e-7;
            double temp = 1 / (a + b * Math.Log(resistance) + c * Math.Log(resistance) * Math.Log(resistance) * Math.Log(resistance)) - 273.2;
            return Math.Round(temp,3);
        }

        private string GetDataValues()
        {
            IDatabase db = this.redis.GetDatabase(0);
            string[] ks = { "", "", "" };//list.Keys.ToArray<string>();

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
            //string stamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
            try
            {
                IDatabase db_raw = this.redis.GetDatabase(0);
                IDatabase db_result = this.redis.GetDatabase(redisDbIndex);

                //RedisKey[] keys = list.Keys.Select(key => (RedisKey)key).ToArray();
                List<RedisKey> keyCollection = new List<RedisKey>();
                foreach (string k in list.Keys)
                {
                    keyCollection.Add(k);
                    //keyCollection.Add(k + "-005");
                    //keyCollection.Add(k + "-012");
                }

                RedisKey[] keys = keyCollection.ToArray();

                RedisValue[] vals = db_raw.StringGet(keys);

                Dictionary<string, DataValue> dvs = new Dictionary<string, DataValue>();

                foreach (RedisValue rv in vals)
                {
                    if (!rv.IsNull)
                    {
                        Bgk_Micro_40A_Data dv = JsonConvert.DeserializeObject<Bgk_Micro_40A_Data>((string)rv);

                        string key = dv.SensorId;

                        double temp = ResistanceToTemperature(dv.Value2);

                        BgkTemperatureConfig ptv = list[key];

                        if (ptv.Stamp != dv.TimeStamp)
                        {
                            ptv.temperature = temp;
                            ptv.Stamp = dv.TimeStamp;

                            DataValue strainDv = new DataValue();
                            strainDv.SensorId = ptv.SensorId;
                            strainDv.TimeStamp = ptv.Stamp;
                            strainDv.ValueType = "005";
                            strainDv.Value = ptv.temperature;
                            string tempKey = ptv.SensorId + "-005";
                            string result = JsonConvert.SerializeObject(strainDv);
                            pair[tempKey] = result;

                            Temperature_Data sd = new Temperature_Data();
                            sd.SensorId = ptv.SensorId;
                            sd.TimeStamp = ptv.Stamp;
                            sd.Temperature = ptv.temperature;

                            string mq_string = JsonConvert.SerializeObject(sd);
                            RabbitMsg msg = new RabbitMsg();
                            msg.RouteKey = ptv.SensorId;
                            msg.Body = mq_string;
                            dataQueue.Enqueue(msg);

                            str += ptv.SensorId + "\r\n";
                            str += " temperature: " + ptv.temperature + "\r\n";
                        }
                    }
                }

                if (pair.Count > 0)
                {
                    db_result.StringSet(pair.ToArray());
                    pair.Clear();
                }
                //lthis.AppendLog(str);
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

