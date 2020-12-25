using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using StackExchange.Redis;
//using System.Data.SQLite;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Collections.Concurrent;
using System.Threading;
using System.IO;
using Newtonsoft.Json;
using System.Configuration;
using RabbitMQ.Client;

namespace DataSolving
{
    public partial class Form1 : Form
    {
        private ConnectionMultiplexer redis;
        private string stamp;
        private int index;
        private ConcurrentQueue<RabbitMsg> dataQueue;
        private BackgroundWorker backgroundWorkerSaveData;
        private bool saveDataSuccess;

        private List<DataSolve> solverCollection;

        public string databaseIp;
        public string databaseUser;
        public string databasePwd;
        public string databaseName;
        public string databaseTable;
        public string redisServerIp;
        private int redisDbIndex;

        public Form1()
        {
            InitializeComponent();

            databaseIp = ConfigurationManager.AppSettings["DatabaseIP"];
            databaseName = ConfigurationManager.AppSettings["DataBase"];
            databaseUser = ConfigurationManager.AppSettings["UserName"];
            databasePwd = ConfigurationManager.AppSettings["Password"];
            databaseTable = ConfigurationManager.AppSettings["Table"];

            redisDbIndex = int.Parse(ConfigurationManager.AppSettings["RedisDbIndex"]);
            redisServerIp = ConfigurationManager.AppSettings["RedisServerIP"];

            //redis = ConnectionMultiplexer.Connect("localhost,abortOnConnect=false");
            string redisConnString = redisServerIp + ",abortConnect=false";
            redis = ConnectionMultiplexer.Connect(redisConnString);

            dataQueue = new ConcurrentQueue<RabbitMsg>();
            solverCollection = new List<DataSolve>();
            LoadConfigs();
            backgroundWorkerSaveData = new BackgroundWorker();
            backgroundWorkerSaveData.WorkerSupportsCancellation = true;
            backgroundWorkerSaveData.DoWork += backgroundWorkerSaveData_DoWork;
            saveDataSuccess = true;

            buttonStart.Enabled = true;
            buttonStop.Enabled = false;
        }

        private void LoadAct4238StrainConfig(SqlConnection connection)
        {
            string strainStatement = "select SensorId,G_C,R0,K,T0,Description from ACT4238StrainConfig";
            SqlCommand strainCommand = new SqlCommand(strainStatement, connection);
            using (SqlDataReader reader = strainCommand.ExecuteReader())
            {
                Dictionary<string, ACT4238StrainConfig> configCollection = new Dictionary<string, ACT4238StrainConfig>();

                while (reader.Read())
                {
                    string sensorId = reader.GetString(0);
                    double g = reader.GetDouble(1);
                    double r0 = reader.GetDouble(2);
                    double k = reader.GetDouble(3);
                    double t0 = reader.GetDouble(4);
                    object desc = reader.GetValue(5);

                    ACT4238StrainConfig ssv = new ACT4238StrainConfig(sensorId, g, r0, k, t0);
                    configCollection.Add(sensorId, ssv);

                    string[] viewItem = { sensorId, sensorId, desc.ToString() };
                    ListViewItem listItem = new ListViewItem(viewItem);
                    this.listView1.Items.Add(listItem);
                }
                ACT4238StrainSolve actSolver = new ACT4238StrainSolve(configCollection, 300, redis, redisDbIndex, textBoxLog,dataQueue);
                solverCollection.Add(actSolver);
            }
        }

        private void LoadBgkStrainConfig(SqlConnection connection)
        {
            string strainStatement = "select SensorId,G_C,R0,K,T0,Description from BgkStrainConfig";
            SqlCommand strainCommand = new SqlCommand(strainStatement, connection);
            using (SqlDataReader reader = strainCommand.ExecuteReader())
            {
                Dictionary<string, BgkStrainConfig> configCollection = new Dictionary<string, BgkStrainConfig>();

                while (reader.Read())
                {
                    string sensorId = reader.GetString(0);
                    double g = reader.GetDouble(1);
                    double r0 = reader.GetDouble(2);
                    double k = reader.GetDouble(3);
                    double t0 = reader.GetDouble(4);
                    object desc = reader.GetValue(5);

                    BgkStrainConfig ssv = new BgkStrainConfig(sensorId, g, r0, k, t0);
                    configCollection.Add(sensorId, ssv);

                    string[] viewItem = { sensorId, sensorId, desc.ToString() };
                    ListViewItem listItem = new ListViewItem(viewItem);
                    this.listView1.Items.Add(listItem);
                }
                BgkStrainSolve actSolver = new BgkStrainSolve(configCollection, 300, redis, redisDbIndex, textBoxLog,dataQueue);
                solverCollection.Add(actSolver);
            }
        }

        private void LoadBgkTemperatureConfig(SqlConnection connection)
        {
            string sqlStatement = "select SensorId,Description from BgkTemperatureConfig";
            SqlCommand command = new SqlCommand(sqlStatement, connection);
            using (SqlDataReader reader = command.ExecuteReader())
            {
                Dictionary<string, BgkTemperatureConfig> configCollection = new Dictionary<string, BgkTemperatureConfig>();

                while (reader.Read())
                {
                    string sensorId = reader.GetString(0);
                    object desc = reader.GetValue(1);

                    BgkTemperatureConfig ssv = new BgkTemperatureConfig(sensorId);
                    configCollection.Add(sensorId, ssv);

                    string[] viewItem = { sensorId, sensorId, desc.ToString() };
                    ListViewItem listItem = new ListViewItem(viewItem);
                    this.listView1.Items.Add(listItem);
                }
                BgkTemperatureSolve actSolver = new BgkTemperatureSolve(configCollection, 300, redis, redisDbIndex, textBoxLog, dataQueue);
                solverCollection.Add(actSolver);
            }
        }

        private void LoadInclinationConfig(SqlConnection connection)
        {
            string sqlStatement = "select SensorId,InitX,InitY,Description from InclinationConfig";
            SqlCommand command = new SqlCommand(sqlStatement, connection);
            using (SqlDataReader reader = command.ExecuteReader())
            {
                Dictionary<string, InclinationConfig> configCollection = new Dictionary<string, InclinationConfig>();

                while (reader.Read())
                {
                    string sensorId = reader.GetString(0);
                    double initX = reader.GetDouble(1);
                    double initY = reader.GetDouble(2);
                    object desc = reader.GetValue(3);

                    InclinationConfig ssv = new InclinationConfig();
                    ssv.SensorId = sensorId;
                    ssv.InitX = initX;
                    ssv.InitY = initY;
                    configCollection.Add(sensorId, ssv);

                    string[] viewItem = { sensorId, sensorId, desc.ToString() };
                    ListViewItem listItem = new ListViewItem(viewItem);
                    this.listView1.Items.Add(listItem);
                }
                InclinationSolve actSolver = new InclinationSolve(configCollection, 300, redis, redisDbIndex, textBoxLog, dataQueue);
                solverCollection.Add(actSolver);
            }
        }

        private void LoadSettlementConfig(SqlConnection connection)
        {
            string sqlStatement = "select SensorId,RefPoint,InitValue,Description from SettlementConfig";
            SqlCommand command = new SqlCommand(sqlStatement, connection);
            using (SqlDataReader reader = command.ExecuteReader())
            {
                Dictionary<string, SettlementConfig> configCollection = new Dictionary<string, SettlementConfig>();

                while (reader.Read())
                {
                    string sensorId = reader.GetString(0);
                    string ref1 = reader.GetString(1);
                    double init = reader.GetDouble(2);
                    string desc = reader.GetString(3);

                    SettlementConfig ssv = new SettlementConfig(sensorId,init,ref1);
                    configCollection.Add(sensorId, ssv);

                    string[] viewItem = { sensorId, sensorId, desc.ToString() };
                    ListViewItem listItem = new ListViewItem(viewItem);
                    this.listView1.Items.Add(listItem);
                }
                SettlementSolve actSolver = new SettlementSolve(configCollection, 300, redis, redisDbIndex, textBoxLog, dataQueue);
                solverCollection.Add(actSolver);
            }
        }

        private void LoadSKD100Config(SqlConnection connection)
        {
            string sqlStatement = "select SensorId,InitVal,Description from SKD100Config";
            SqlCommand command = new SqlCommand(sqlStatement, connection);
            using (SqlDataReader reader = command.ExecuteReader())
            {
                Dictionary<string, SKD100Config> configCollection = new Dictionary<string, SKD100Config>();

                while (reader.Read())
                {
                    string sensorId = reader.GetString(0);
                    double init = reader.GetDouble(1);
                    object desc = reader.GetValue(2);

                    SKD100Config ssv = new SKD100Config();
                    ssv.SensorId = sensorId;
                    ssv.InitVal = init;
                    configCollection.Add(sensorId, ssv);

                    string[] viewItem = { sensorId, sensorId, desc.ToString() };
                    ListViewItem listItem = new ListViewItem(viewItem);
                    this.listView1.Items.Add(listItem);
                }
                SKD100Solve actSolver = new SKD100Solve(configCollection, 400, redis, redisDbIndex, textBoxLog, dataQueue);
                solverCollection.Add(actSolver);
            }
        }

        private void LoadTemperatureHumidityConfig(SqlConnection connection)
        {
            string sqlStatement = "select SensorId,Description from TemperatureHumidityConfig";
            SqlCommand command = new SqlCommand(sqlStatement, connection);
            using (SqlDataReader reader = command.ExecuteReader())
            {
                Dictionary<string, TemperatureHumidityConfig> configCollection = new Dictionary<string, TemperatureHumidityConfig>();

                while (reader.Read())
                {
                    string sensorId = reader.GetString(0);
                    object desc = reader.GetValue(1);

                    TemperatureHumidityConfig ssv = new TemperatureHumidityConfig();
                    ssv.SensorId = sensorId;
                    configCollection.Add(sensorId, ssv);

                    string[] viewItem = { sensorId, sensorId, desc.ToString() };
                    ListViewItem listItem = new ListViewItem(viewItem);
                    this.listView1.Items.Add(listItem);
                }
                TemperatureHumiditySolve actSolver = new TemperatureHumiditySolve(configCollection, 300, redis, redisDbIndex, textBoxLog, dataQueue);
                solverCollection.Add(actSolver);
            }
        }


        private void LoadConfigs()
        {
            string connectionString = "Data Source = " + databaseIp + ";Network Library = DBMSSOCN;Initial Catalog = " + databaseName + ";User ID = " + databaseUser + ";Password = " + databasePwd;

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                try
                {
                    connection.Open();
                    LoadAct4238StrainConfig(connection);
                    LoadBgkStrainConfig(connection);
                    LoadBgkTemperatureConfig(connection);
                    LoadInclinationConfig(connection);
                    LoadSettlementConfig(connection);
                    LoadSKD100Config(connection);
                    LoadTemperatureHumidityConfig(connection);
                }
                catch (Exception ex)
                {
                    connection.Close();
                    MessageBox.Show(ex.Message);
                    return;
                }
                connection.Close();
            }
        }
        /*
        private void LoadSubtractionConfig()
        {
            string database = "Data Source = config.db";

            using (SQLiteConnection connection = new SQLiteConnection(database))
            {
                connection.Open();

                List<int> periods = new List<int>();

                string sqlStatement = "SELECT DISTINCT Period from SensorInfo";
                SQLiteCommand command1 = new SQLiteCommand(sqlStatement, connection);
                using (SQLiteDataReader reader1 = command1.ExecuteReader())
                {
                    while (reader1.Read())
                    {
                        int port = reader1.GetInt32(0);
                        periods.Add(port);
                    }
                }

                foreach (int port in periods)
                {
                    Dictionary<string, SimpleSubtractionValue> subtractionChannels = new Dictionary<string, SimpleSubtractionValue>();
                    
                    int period = 0;
                    sqlStatement = "select Key,InitValue,ValueType ,Period from SubtractionConfig where Period =" + port.ToString();
                    command1 = new SQLiteCommand(sqlStatement, connection);
                    using (SQLiteDataReader reader = command1.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            string key = reader.GetString(0);
                            double init = reader.GetDouble(1);
                            string type = reader.GetString(2);
                            period = reader.GetInt32(3);
                            SimpleSubtractionValue ssv = new SimpleSubtractionValue(type, init);
                            subtractionChannels.Add(key, ssv);
                        }
                    }
                    SimpleSubtractionSolve ss= new SimpleSubtractionSolve(subtractionChannels, period, redis, textBoxLog);
                    solverCollection.Add(ss);
                }

                connection.Close();
            }
        }*/
        /*
        public void UpdateDatabaseSetting(string ip,string user,string pwd,string database,string table)
        {
            this.databaseIp = ip;
            this.databaseUser = user;
            this.databasePwd = pwd;
            this.databaseName = database;
            this.databaseTable = table;

            using (SQLiteConnection connection = new SQLiteConnection("Data Source = config.db"))
            {
                connection.Open();
                string strainStatement = "delete from dbconfig";
                SQLiteCommand command = new SQLiteCommand(strainStatement, connection);
                command.ExecuteNonQuery();

                strainStatement = "insert into dbconfig values('"+databaseIp+"','"+databaseUser + "','" +databasePwd + "','" +databaseName + "','" +databaseTable + "')";
                command = new SQLiteCommand(strainStatement, connection);
                command.ExecuteNonQuery();
                connection.Close();
            }
        }*/

        private void buttonStart_Click(object sender, EventArgs e)
        {
            buttonStart.Enabled = false;
            buttonStop.Enabled = true;
            foreach(DataSolve item in solverCollection)
            {
                string objType = item.GetType().ToString();
                //if(objType== "DataSolving.ACT4238StrainSolve")
                {
                    item.Start();
                    //AppendLog(objType + " start");
                }
                //item.Start();
                //break;
            }
            backgroundWorkerSaveData.RunWorkerAsync();
            //AppendLog("backgroundWorkerSaveData starting");
        }

        private void buttonStop_Click(object sender, EventArgs e)
        {
            buttonStart.Enabled = true;
            buttonStop.Enabled = false;
            foreach (DataSolve item in solverCollection)
            {
                item.Stop();
            }
            backgroundWorkerSaveData.CancelAsync();
        }

        private bool InitialConnect(ConnectionFactory factory,ref IConnection connection)
        {
            bool result = false;
            try
            {
                connection = factory.CreateConnection();
                result = true;
            }
            catch (Exception e)
            {//RabbitMQ.Client.Exceptions.BrokerUnreachableException e
                textBoxLog.AppendText(e.Message + "\r\n");
                result = false;
            }
            return result;
        }

        private string GetRouteKey(string sensorId)
        {
            string key = sensorId.Substring(0, 8) + "." + sensorId.Substring(8, 2) + "." + sensorId.Substring(10);
            return key;
        }

        private void backgroundWorkerSaveData_DoWork(object sender, DoWorkEventArgs e)
        {
            BackgroundWorker bgWorker = sender as BackgroundWorker;

            //AppendLog("backgroundWorkerSaveData started");

            ConnectionFactory factory = new ConnectionFactory() { HostName = "localhost" };
            
            IConnection connection = null;
            //If initial client connection to a RabbitMQ node fails, automatic connection recovery won't kick in.
            while (!InitialConnect(factory,ref connection))
            {
                AppendLog("rabbit initial reconnect in 5 seconds\r\n");
                Thread.Sleep(5000);
            }
            IModel channel = connection.CreateModel();
            channel.ConfirmSelect();
            //the following statement get stuck in win 2008
            //channel.ExchangeDeclare(exchange: "BridgeDataEx", type: "topic");
            while (true)
            {
                try
                {
                    int dataCount = dataQueue.Count;
                    if (connection.IsOpen)
                    {
                        if (channel.IsClosed)
                        {
                            channel = connection.CreateModel();
                            channel.ConfirmSelect();
                        }
                        RabbitMsg dvs;
                        //bool success = dataQueue.TryDequeue(out dvs);
                        bool success = dataQueue.TryPeek(out dvs);
                        if (success)
                        {
                            if (channel.IsOpen)
                            {
                                var properties = channel.CreateBasicProperties();
                                properties.Persistent = true;
                                
                                var body = Encoding.UTF8.GetBytes(dvs.Body);
                                var routingKey = GetRouteKey(dvs.RouteKey);
                                channel.BasicPublish(exchange: "BridgeDataEx",
                                                     routingKey: routingKey,
                                                     basicProperties: properties,
                                                     body: body);
                                bool confirmSuccess = channel.WaitForConfirms();
                                if (confirmSuccess)
                                {
                                    dataQueue.TryDequeue(out dvs);
                                }
                            }
                            else
                            {
                                AppendLog("rabbit channel is closed\r\n");
                            }
                            
                        }
                        else
                        {
                            //AppendLog("dataQueue.TryDequeue failed!");
                        }
                    }
                    else
                    {
                        AppendLog("rabbit connection is closed,auto reconnect.....");
                    }
                    if (dataQueue.Count == 0)
                    {
                        Thread.Sleep(30000);
                    }
                }
                catch (Exception ex)
                {
                    AppendLog(ex.StackTrace);
                    using (StreamWriter sw = new StreamWriter(@"ErrLog.txt", true))
                    {
                        sw.WriteLine(ex.Message + " \r\n" + ex.StackTrace.ToString());
                        sw.WriteLine("---------------------------------------------------------");
                        sw.Close();
                    }
                }
                
                if (bgWorker.CancellationPending == true)
                {
                    AppendLog("backgroundWorkerSaveData_DoWork Exit");
                    connection.Close();
                    e.Cancel = true;
                    break;
                }

                
                //Thread.Sleep(3000);
            }
        }

        public void AppendLog(string content)
        {
            if (textBoxLog.InvokeRequired)
            {
                textBoxLog.BeginInvoke(new MethodInvoker(() =>
                {
                    textBoxLog.AppendText(content + "\r\n");
                }));
            }
            else
            {
                textBoxLog.AppendText(content + "\r\n");
            }
        }

        public void Ma(string[] args)
        {
            var factory = new ConnectionFactory() { HostName = "localhost" };
            
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.ExchangeDeclare(exchange: "topic_logs",
                                        type: "topic");
                if (connection.IsOpen)
                {
                    
                }
                if (channel.IsOpen)
                {

                }

                var routingKey = (args.Length > 0) ? args[0] : "anonymous.info";
                var message = (args.Length > 1)
                              ? string.Join(" ", args.Skip(1).ToArray())
                              : "Hello World!";
                var body = Encoding.UTF8.GetBytes(message);
                channel.BasicPublish(exchange: "topic_logs",
                                     routingKey: routingKey,
                                     basicProperties: null,
                                     body: body);
                //Console.WriteLine(" [x] Sent '{0}':'{1}'", routingKey, message);
            }
        }

        private void BackgroundWorkerSaveData_DoWorkX(object sender, DoWorkEventArgs e)
        {
            BackgroundWorker bgWorker = sender as BackgroundWorker;

            DataTable dt = GetTableSchema();

            while (true)
            {
                try
                {
                    int dataCount = dataQueue.Count;

                    if (dataCount > 100)
                    {
                        if (saveDataSuccess)
                        {
                            dt.Clear();
                        }
                        
                        for (int i = 0; i < dataCount; i++)
                        {
                            //DataValue dv=null;
                            //string dvs;
                            //bool success = dataQueue.TryDequeue(out dvs);
                            //if (success)
                            //{
                            //    DataRow row = dt.NewRow();
                            //    row[0] = dv.SensorId;
                            //    row[1] = DateTime.Parse(dv.TimeStamp);
                            //    row[2] = dv.ValueType;
                            //    row[3] = dv.Value;
                            //    dt.Rows.Add(row);
                            //}
                        }
                        InsertData(dt, "data");
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



                if (bgWorker.CancellationPending == true)
                {
                    e.Cancel = true;
                    break;
                }

                Thread.Sleep(3000);
            }
        }

        private DataTable GetTableSchema()
        {
            DataTable dt = new DataTable();
            dt.Columns.AddRange(new DataColumn[] {
                //new DataColumn("ID",typeof(int)),
                new DataColumn("SensorId",typeof(string)),
                new DataColumn("Stamp",typeof(System.DateTime)),
                new DataColumn("Type",typeof(string)),
                new DataColumn("Value",typeof(Single))
            });
            return dt;
        }

        private void InsertData(DataTable dt, string tableName)
        {
            string connectionString = "Data Source = "+databaseIp+";Network Library = DBMSSOCN;Initial Catalog = "+databaseName+";User ID = "+databaseUser+";Password = "+databasePwd;

            Stopwatch sw = new Stopwatch();

            try
            {
                using (SqlConnection conn = new SqlConnection(connectionString))
                {
                    SqlBulkCopy bulkCopy = new SqlBulkCopy(conn);
                    bulkCopy.DestinationTableName = tableName;
                    bulkCopy.BatchSize = dt.Rows.Count;
                    //bulkCopy.BulkCopyTimeout
                    conn.Open();
                    sw.Start();

                    if (dt != null && dt.Rows.Count != 0)
                    {
                        bulkCopy.WriteToServer(dt);
                        sw.Stop();
                    }
                    textBoxLog.AppendText(string.Format("插入{0}条记录共花费{1}毫秒，{2}分钟", dt.Rows.Count, sw.ElapsedMilliseconds, sw.ElapsedMilliseconds / 1000 / 60));
                    conn.Close();
                    saveDataSuccess = true;
                }
            }
            catch (Exception ex)
            {
                saveDataSuccess = false;
                if (textBoxLog.InvokeRequired)
                {
                    textBoxLog.BeginInvoke(new MethodInvoker(() =>
                    {
                        textBoxLog.AppendText(ex.Message + "\r\n");
                    }));
                }
                else
                {
                    textBoxLog.AppendText(ex.Message + "\r\n");
                }
            }    
        }

        private void ToolStripMenuDatabaseItem_Click(object sender, EventArgs e)
        {
            DatabaseConfig dlg = new DatabaseConfig(this);
            dlg.ShowDialog();
        }

        private void buttonTest_Click(object sender, EventArgs e)
        {
            MessageBox.Show("dataQueue.Count = "+dataQueue.Count.ToString());
            //MessageBox.Show(GetRouteKey("5000000211001"));
            return;
            string serverIP = ConfigurationManager.AppSettings["DatabaseIP"];
            string dataBase = ConfigurationManager.AppSettings["DataBase"];
            string user = ConfigurationManager.AppSettings["UserName"];
            string password = ConfigurationManager.AppSettings["Password"];

            string json ="{\"SensorId\": \"5000000211001\",\"TimeStamp\": \"2020-04-04 22:50:24\",\"Value1\": 123.4,\"Value2\": -0.005,\"Value3\": -0.005}";

            var obj = JsonConvert.DeserializeObject(json);
            

            textBoxLog.AppendText(serverIP+"\r\n");
            textBoxLog.AppendText(dataBase + "\r\n");
            textBoxLog.AppendText(user + "\r\n");
            textBoxLog.AppendText(password + "\r\n");
        }

        //private void textBoxLog_TextChanged(object sender, EventArgs e)
        //{

        //}
    }
}
