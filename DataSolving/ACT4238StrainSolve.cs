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
    public class ACT4238StrainConfig
    {
        public string SensorId;
        public double G;
        public double R0;
        public double K;
        public double T0;
        public double frequency;
        public double temperature;
        public bool isUpdated;
        public string Stamp;
        public ACT4238StrainConfig(string sensorId, double G, double R0,double K,double T0)
        {
            this.SensorId = sensorId;
            this.G = G;
            this.R0 = R0;
            this.K = K;
            this.T0 = T0;
            this.frequency = 0;
            this.temperature = 0;
            this.isUpdated = false;
            this.Stamp = "";
        }
    }

    class ACT4238StrainSolve:DataSolve
    {
        private System.Timers.Timer timer;
        private BackgroundWorker backgroundWorker;
        private Dictionary<string, ACT4238StrainConfig> list;
        private Dictionary<string, string> stamp;
        private int redisDbIndex;
        private ConcurrentQueue<RabbitMsg> dataQueue;
        public ACT4238StrainSolve(Dictionary<string, ACT4238StrainConfig> keys,int period,ConnectionMultiplexer redis, int redisIndex, TextBox log, ConcurrentQueue<RabbitMsg> queue) :base(redis,log)
        {
            this.dataQueue = queue;
            list = keys;
            timer = new System.Timers.Timer(period*1000);
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

        private double CalculateStrain(ACT4238StrainConfig ssv)
        {
            double Digit = ssv.frequency * ssv.frequency / 1000;
            double currentValue = ssv.G * (Digit - ssv.R0) + ssv.K * (ssv.temperature - ssv.T0) ;
            return Math.Round(currentValue, 3);
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
            str+= stamp+" ";
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
                foreach(string k in list.Keys)
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
                        ACT4238_Data dv = JsonConvert.DeserializeObject<ACT4238_Data>((string)rv);
                        
                        string key = dv.SensorId;

                        ACT4238StrainConfig ptv = list[key];

                        if (ptv.Stamp != dv.TimeStamp)
                        {
                            ptv.isUpdated = true;
                            ptv.frequency = dv.Frequency;
                            ptv.temperature = dv.Temperature;
                            ptv.Stamp = dv.TimeStamp;
                        }
                    }
                }

                foreach (string key in list.Keys)
                {
                    //string str = "";

                    ACT4238StrainConfig ptv = list[key];

                    if (ptv.isUpdated )
                    {
                        if (ptv.temperature == 0 || ptv.frequency == 0)
                        {
                            this.AppendLog(stamp+" "+ptv.SensorId + "This Channel is broken");
                            continue;
                        }
                        string strainNormalKey = ptv.SensorId + "-009";

                        double strain = CalculateStrain(ptv);

                        DataValue strainDv = new DataValue();
                        strainDv.SensorId = ptv.SensorId;
                        strainDv.TimeStamp = ptv.Stamp;
                        strainDv.ValueType = "009";
                        strainDv.Value = strain;

                        string result = JsonConvert.SerializeObject(strainDv);
                        pair[strainNormalKey] = result;

                        strainDv.ValueType = "005";
                        strainDv.Value = ptv.temperature;
                        string tempKey = ptv.SensorId + "-005";
                        result = JsonConvert.SerializeObject(strainDv);
                        pair[tempKey] = result;

                        Strain_Data sd = new Strain_Data();
                        sd.SensorId = ptv.SensorId;
                        sd.TimeStamp = ptv.Stamp;
                        sd.Frequency = ptv.frequency;
                        sd.Strain = strain;
                        sd.Temperature = ptv.temperature;

                        string mq_string = JsonConvert.SerializeObject(sd);
                        RabbitMsg msg = new RabbitMsg();
                        msg.RouteKey = ptv.SensorId;
                        msg.Body = mq_string;
                        dataQueue.Enqueue(msg);

                        ptv.isUpdated = false;

                        str += ptv.SensorId + "\r\n";
                        str += "R0: " + ptv.R0.ToString() + " T0: " + ptv.T0 + " G: " + ptv.G + " K: " + ptv.K + "\r\n";
                        str += "frequency: " + ptv.frequency + " temperature: " + ptv.temperature + "\r\n";
                        str += "Strain: " + strain.ToString() + "\r\n";
                        ////lthis.AppendLog(str);

                        if (bgWorker.CancellationPending == true)
                        {
                            if (pair.Count > 0)
                            {
                                db_result.StringSet(pair.ToArray());
                                pair.Clear();
                            }
                            e.Cancel = true;
                            break;
                        }
                        //str += temp.SensorId + " " + temp.TimeStamp + " " + temp.ValueType + " " + temp.Value + "\r\n";
                        
                        //Thread.Sleep(500);
                    }
                    else
                    {
                        //this.AppendLog(stamp + key + "is not updated");
                        continue;
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
                    sw.WriteLine(stamp +" "+ ex.Message + " \r\n" + ex.StackTrace.ToString());
                    sw.WriteLine("---------------------------------------------------------");
                    sw.Close();
                }
            }
        }
    }
}
