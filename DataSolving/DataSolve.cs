using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using StackExchange.Redis;
//using System.Data.SQLite;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Collections.Concurrent;
using System.Threading;
using System.IO;
using System.ComponentModel;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace DataSolving
{
    class DataSolve
    {
        protected ConnectionMultiplexer redis;
        private TextBox logger;
        public DataSolve(ConnectionMultiplexer redis,TextBox log)
        {
            this.redis = redis;
            this.logger = log;
        }
        public virtual void Start() { }
        public virtual void Stop() { }
        public void AppendLog(string content)
        {
            if (logger.InvokeRequired)
            {
                logger.BeginInvoke(new MethodInvoker(() =>
                {
                    logger.AppendText(content+"\r\n");
                }));
            }
            else
            {
                logger.AppendText(content + "\r\n");
            }
        }
    }
}
