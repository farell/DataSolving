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
    class Young81000Value
    {
        public string SensorId;
        public string SpeedKey;
        public string AngleKey;
        public string HorizontalSpeedType;
        public string VerticalSpeedType;

        public Young81000Value(string sensorId,string speedKey,string angleKey,string horizontalSpeedType,string verticalSpeedType)
        {
            this.SensorId = sensorId;
            this.SpeedKey = speedKey;
            this.AngleKey = angleKey;
            this.HorizontalSpeedType = horizontalSpeedType;
            this.VerticalSpeedType = verticalSpeedType;
        }
    }

    class Young81000Slove:DataSolve
    {
        private System.Timers.Timer timer;
        private BackgroundWorker backgroundWorker;
        private Dictionary<string, Young81000Value> list;
        private Dictionary<string, string> stamp;
        public Young81000Slove(Dictionary<string, Young81000Value> keys, int period, ConnectionMultiplexer redis, TextBox log) : base(redis, log)
        {
            list = keys;
            timer = new System.Timers.Timer(period * 1000);
            stamp = new Dictionary<string, string>();
            timer.Elapsed += Timer_Elapsed;
            InitStamp();
            backgroundWorker = new BackgroundWorker();
            backgroundWorker.WorkerSupportsCancellation = true;
            backgroundWorker.DoWork += BackgroundWorker_DoWork;
        }

        private void InitStamp()
        {
            foreach (string key in list.Keys)
            {
                stamp.Add(list[key].SpeedKey, "");
                stamp.Add(list[key].AngleKey, "");
            }
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
            //redis.Close();
            timer.Stop();
            backgroundWorker.CancelAsync();
        }

        private DataValue GetDataValue(IDatabase db, string key)
        {
            string str = "";
            byte[] receiveBytes = db.StringGet(key);
            if (receiveBytes == null)
            {
                str = "key " + key + " not exist\r\n";
                return null;
            }
            else
            {
                DataValue dv = JsonConvert.DeserializeObject<DataValue>(System.Text.Encoding.Default.GetString(receiveBytes));;

                if (dv.SensorId == null)
                {
                    str = "maleformed packet";
                    return null;
                }
                else
                {
                    string st = "";
                    try
                    {
                        st = stamp[key];
                        if (st == dv.TimeStamp)
                        {
                            str = "data not updated!\r\n";
                            return null;
                        }
                        else
                        {
                            stamp[key] = dv.TimeStamp;
                            str = dv.SensorId + " " + dv.TimeStamp + " " + dv.ValueType + " " + dv.Value + "\r\n";
                            return dv;
                            //this.dataQueue.Enqueue(dv);
                            //this.logger.AppendText(str);
                        }
                    }
                    catch (Exception ex)
                    {
                        this.AppendLog(key + " " + ex.ToString() + " \r\n" + ex.StackTrace.ToString());
                        return null;
                    }
                }
            }
        }

        private void BackgroundWorker_DoWork(object sender, DoWorkEventArgs e)
        {
            BackgroundWorker bgWorker = sender as BackgroundWorker;

            try
            {
                IDatabase db = this.redis.GetDatabase(0);

                foreach(string key in list.Keys)
                {
                    Young81000Value yv = list[key];

                    DataValue speed3d = GetDataValue(db, yv.SpeedKey);
                    DataValue angle = GetDataValue(db, yv.AngleKey);
                    if (speed3d == null)
                    {
                        this.AppendLog(yv.SpeedKey + " is null");
                        continue;
                    }

                    if (angle == null)
                    {
                        this.AppendLog(yv.AngleKey + " is null");
                        continue;
                    }

                    double speedHorizontal = Math.Round(speed3d.Value * Math.Cos(angle.Value * Math.PI / 180),3);
                    double speedVertical = Math.Round(speed3d.Value * Math.Sin(angle.Value * Math.PI / 180),3);
                    string speedHorizontalKey = yv.SensorId + "-" + yv.HorizontalSpeedType;
                    string speedVerticalKey = yv.SensorId + "-" + yv.VerticalSpeedType;

                    DataValue dv = new DataValue();
                    dv.SensorId = yv.SensorId;
                    dv.TimeStamp = speed3d.TimeStamp;
                    dv.ValueType = yv.HorizontalSpeedType;
                    dv.Value = speedHorizontal;

                    string result = JsonConvert.SerializeObject(dv);
                    db.StringSet(speedHorizontalKey, result);

                    dv.ValueType = yv.VerticalSpeedType;
                    dv.Value = speedVertical;
                    result = JsonConvert.SerializeObject(dv);
                    db.StringSet(speedVerticalKey, result);

                    this.AppendLog("horizontal speed: " + speedHorizontal + " vertical speed: " + speedVertical);

                    if (bgWorker.CancellationPending == true)
                    {
                        e.Cancel = true;
                        break;
                    }
                }
            }
            catch (Exception ex)
            {
                using (StreamWriter sw = new StreamWriter(@"ErrLog.txt", true))
                {
                    sw.WriteLine(ex.Message + " \r\n" + ex.StackTrace.ToString());
                    sw.WriteLine("---------------------------------------------------------");
                    sw.Close();
                }
            }
        }
    }
}
