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
using System.Timers;
using System.Windows.Forms;

namespace DataSolving
{
    public class SettlementConfig
    {
        public string SensorId;
        public double InitValue;
        public string RefPoint;
        public double Value;
        public bool IsUpdated;
        public string Stamp;
        public SettlementConfig(string sensorId, double initValue, string refPoint)
        {
            this.SensorId = sensorId;
            this.InitValue = initValue;
            this.RefPoint = refPoint;
            this.Value = 0;
            this.IsUpdated = false;
            this.Stamp = "";
        }
    }

    class SettlementSolve : DataSolve
    {
        private System.Timers.Timer timer;
        private BackgroundWorker backgroundWorker;
        private Dictionary<string, SettlementConfig> list;
        private Dictionary<string, string> stamp;
        private int redisDbIndex;
        private ConcurrentQueue<RabbitMsg> dataQueue;
        public SettlementSolve(Dictionary<string, SettlementConfig> keys, int period, ConnectionMultiplexer redis, int redisIndex, TextBox log, ConcurrentQueue<RabbitMsg> queue) : base(redis, log)
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
            if (backgroundWorker.IsBusy)
            {
                backgroundWorker.CancelAsync();
            }
        }

        private void BackgroundWorker_DoWork(object sender, DoWorkEventArgs e)
        {
            BackgroundWorker bgWorker = sender as BackgroundWorker;
            string stamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
            string str = "";
            str += stamp+" ";
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
                    //keyCollection.Add(k + "-020");
                }

                RedisKey[] keys = keyCollection.ToArray();

                RedisValue[] vals = db_raw.StringGet(keys);

                foreach (RedisValue rv in vals)
                {
                    if (!rv.IsNull)
                    {
                        BGK3475DM_Data dv = JsonConvert.DeserializeObject<BGK3475DM_Data>((string)rv);

                        string inageKey = dv.SensorId;

                        SettlementConfig ptv = list[inageKey];
                        if (ptv.Stamp != dv.TimeStamp)
                        {
                            ptv.IsUpdated = true;
                            ptv.Value = dv.Innage;
                            ptv.Stamp = dv.TimeStamp;
                        }
                    }
                }

                foreach (string key in list.Keys)
                {
                    SettlementConfig ptv = list[key];

                    if (ptv.IsUpdated == false)
                    {
                        //this.AppendLog(stamp + " " + key + "is not updated");
                        continue;
                    }
                    str += "InitValue: " + ptv.InitValue.ToString() + "\r\n";
                    str += ptv.SensorId + " " + ptv.Stamp + " " + "020" + " " + ptv.Value + "\r\n";

                    string offsetKey = ptv.SensorId + "-007";

                    double offset = Math.Round(ptv.Value - ptv.InitValue, 3);

                    DataValue temp = new DataValue();
                    temp.SensorId = ptv.SensorId;
                    temp.TimeStamp = ptv.Stamp;
                    temp.ValueType = "007";
                    temp.Value = offset;

                    string result = JsonConvert.SerializeObject(temp);
                    pair[offsetKey] = result;

                    temp.ValueType = "020";
                    temp.Value = ptv.Value;
                    string innageKey = ptv.SensorId + "-020";
                    result = JsonConvert.SerializeObject(temp);
                    pair[innageKey] = result;

                    Settlement_Data sd = new Settlement_Data();
                    sd.SensorId = ptv.SensorId;
                    sd.TimeStamp = ptv.Stamp;
                    sd.Innage = ptv.Value;
                    sd.DeltaInnage = offset;

                    string mq_string = "";

                    //db.StringSet(offsetKey, result);
                    str += temp.SensorId + " " + temp.TimeStamp + " " + temp.ValueType + " " + temp.Value + "\r\n";
                    if (ptv.RefPoint == "null")
                    {
                        mq_string = JsonConvert.SerializeObject(sd);
                        //send mq_string
                        RabbitMsg rmsg = new RabbitMsg();
                        rmsg.RouteKey = ptv.SensorId;
                        rmsg.Body = mq_string;
                        dataQueue.Enqueue(rmsg);

                        //pair[ptv.SensorId] = mq_string;
                        //lthis.AppendLog(str);
                        continue;
                    }

                    string deflectionKey = ptv.SensorId + "-010";

                    string refKey = ptv.RefPoint.Split('-')[0];

                    SettlementConfig refPoint = list[refKey];
                    if (!refPoint.IsUpdated)
                    {
                        mq_string = JsonConvert.SerializeObject(sd);
                        //send mq_string
                        RabbitMsg rmsg = new RabbitMsg();
                        rmsg.RouteKey = ptv.SensorId;
                        rmsg.Body = mq_string;
                        dataQueue.Enqueue(rmsg);

                        //pair[ptv.SensorId] = mq_string;
                        //this.AppendLog(stamp + " " + ptv.RefPoint + "ref point is not updated");
                        continue;
                    }

                    double refOffset = Math.Round(refPoint.Value - refPoint.InitValue, 3);

                    double deflection = Math.Round(offset - refOffset, 3);


                    //str += temp.SensorId + " " + temp.TimeStamp + " " + temp.ValueType + " " + temp.Value + "\r\n";

                    temp.ValueType = "010";
                    temp.Value = deflection;
                    result = JsonConvert.SerializeObject(temp);
                    pair[deflectionKey] = result;

                    sd.Deflection = deflection;

                    mq_string = JsonConvert.SerializeObject(sd);
                    //send mq_string
                    RabbitMsg msg = new RabbitMsg();
                    msg.RouteKey = ptv.SensorId;
                    msg.Body = mq_string;
                    dataQueue.Enqueue(msg);

                    //pair[ptv.SensorId] = mq_string;

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
                    SettlementConfig ptv = list[key];
                    ptv.IsUpdated = false;
                }
            }
            catch (Exception ex)
            {
                this.AppendLog(ex.Message);
                using (StreamWriter sw = new StreamWriter(@"ErrLog.txt", true))
                {
                    sw.WriteLine(stamp+" "+ex.Message + " \r\n" + ex.StackTrace.ToString());
                    sw.WriteLine("---------------------------------------------------------");
                    sw.Close();
                }
            }
        }

        /// <summary>
        /// 返回Pi相对于参考点P2的位移
        /// </summary>
        /// <param name="p1">参考点P1</param>
        /// <param name="p2">参考点P2</param>
        /// <param name="pi">测点Pi</param>
        /// <returns></returns>
        private double CalculateOffset(double p1, double p2, double pi)
        {
            //unit mm
            double L = 1393.308;

            double offset = L * (pi - p2) / (p2 - p1);

            return offset;
        }

        /// <summary>
        /// 返回Pi点的挠度（相对于自身按照时刻的位移）
        /// </summary>
        /// <param name="initValue">安装时刻初始值</param>
        /// <param name="pi">测点Pi</param>
        /// <returns></returns>
        private double CalculateDeflection(double pi, double initValue)
        {
            //unit mm

            double deflection = pi - initValue;

            return deflection;
        }
    }
}
