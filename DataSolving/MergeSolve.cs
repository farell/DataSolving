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
    class MergeValue
    {
        public string SensorId { get; set; }
        public string TimeStamp { get; set; }
        public string ValueType { get; set; }
        public double Value { get; set; }
        public bool IsUpdated { get; set; }
    }
    class MergeSolve : DataSolve
    {
        private System.Timers.Timer timer;
        private BackgroundWorker backgroundWorker;
        private Dictionary<string, MergeValue> list;
        private Dictionary<string, string> stamp;
        private int redisDbIndex;
        public MergeSolve(Dictionary<string, MergeValue> keys, int period, ConnectionMultiplexer redis, int redisIndex, TextBox log) : base(redis, log)
        {
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
                    // 56000001 21 003
                    string deviceType = k.Substring(8,2);
                    switch (deviceType)
                    {
                        case "02":
                            keyCollection.Add(k + "-001");
                            keyCollection.Add(k + "-002");
                            break;
                        default:break;
                    }
                    keyCollection.Add(k);
                }

                RedisKey[] keys = keyCollection.ToArray();

                RedisValue[] vals = db_raw.StringGet(keys);

                foreach (RedisValue rv in vals)
                {
                    if (!rv.IsNull)
                    {
                        DataValue dv = JsonConvert.DeserializeObject<DataValue>((string)rv);

                        string inageKey = dv.SensorId + "-" + dv.ValueType;

                        MergeValue ptv = list[inageKey];
                        if (ptv.TimeStamp != dv.TimeStamp)
                        {
                            ptv.IsUpdated = true;
                            ptv.SensorId = dv.SensorId;
                            ptv.Value = dv.Value;
                            ptv.ValueType = dv.ValueType;
                            ptv.TimeStamp = dv.TimeStamp;
                        }
                    }
                }

                foreach (string key in list.Keys)
                {
                    MergeValue ptv = list[key];

                    if (ptv.IsUpdated == false)
                    {
                        this.AppendLog(stamp + " " + key + "is not updated");
                        continue;
                    }
                    //str += "InitValue: " + ptv.InitValue.ToString() + "\r\n";
                    //str += ptv.SensorId + " " + ptv.Stamp + " " + "020" + " " + ptv.Value + "\r\n";

                    string offsetKey = ptv.SensorId + "-" + ptv.ValueType;

                    //double offset = Math.Round(ptv.Value - ptv.InitValue, 3);

                    DataValue temp = new DataValue();
                    temp.SensorId = ptv.SensorId;
                    temp.TimeStamp = ptv.TimeStamp;
                    temp.ValueType = ptv.ValueType;
                    temp.Value = ptv.Value;

                    string result = JsonConvert.SerializeObject(temp);
                    pair[key] = result;

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
                    str += temp.SensorId + " " + temp.TimeStamp + " " + temp.ValueType + " " + temp.Value + "\r\n";
                    //lthis.AppendLog(str);
                }
                if (pair.Count > 0)
                {
                    db_result.StringSet(pair.ToArray());
                    pair.Clear();
                }

                foreach (string key in list.Keys)
                {
                    MergeValue ptv = list[key];
                    ptv.IsUpdated = false;
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


