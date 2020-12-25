using System;
using System.Collections.Generic;
using System.Text;

namespace DataSolving
{
    public class DataValue
    {
        public string SensorId { get; set; }
        public string TimeStamp { get; set; }
        public string ValueType { get; set; }
        public double Value { get; set; }
    }

    public class RS_WS_N01_2x_Data
    {
        public string SensorId { get; set; }
        public string TimeStamp { get; set; }
        public double Temperature { get; set; }
        public double Humidity { get; set; }
    }

    public class AS109_Data
    {
        public string SensorId { get; set; }
        public string TimeStamp { get; set; }
        public double Temperature { get; set; }
        public double Humidity { get; set; }
    }

    public class Bgk_Micro_40A_Data
    {
        public string SensorId { get; set; }
        public string TimeStamp { get; set; }
        public double Value1 { get; set; }
        public double Value2 { get; set; }
    }

    public class JMWS1D_Data
    {
        public string SensorId { get; set; }
        public string TimeStamp { get; set; }
        public double Temperature { get; set; }
        public double Humidity { get; set; }
    }

    public class ACA826T_Data
    {
        public string SensorId { get; set; }
        public string TimeStamp { get; set; }
        public double X { get; set; }
        public double Y { get; set; }
    }

    public class ACT4238_Data
    {
        public string SensorId { get; set; }
        public string TimeStamp { get; set; }
        public double Temperature { get; set; }
        public double Frequency { get; set; }
    }

    public class BGK3475DM_Data
    {
        public string SensorId { get; set; }
        public string TimeStamp { get; set; }
        public double Innage { get; set; }
    }

    public class MDL62XXAT_Data
    {
        public string SensorId { get; set; }
        public string TimeStamp { get; set; }
        public double Innage { get; set; }
    }

    public class SKD100_Data
    {
        public string SensorId { get; set; }
        public string TimeStamp { get; set; }
        public double Distance { get; set; }
    }

    public class LaserRange_Data
    {
        public string SensorId { get; set; }
        public string TimeStamp { get; set; }
        public double Distance { get; set; }
        public double DeltaDistance { get; set; }
    }

    public class Settlement_Data
    {
        public string SensorId { get; set; }
        public string TimeStamp { get; set; }
        public double Innage { get; set; }
        public double DeltaInnage { get; set; }
        public double Deflection { get; set; }
    }

    public class Strain_Data
    {
        public string SensorId { get; set; }
        public string TimeStamp { get; set; }
        public double Temperature { get; set; }
        public double Frequency { get; set; }
        public double Strain { get; set; }
    }

    public class Temperature_Data
    {
        public string SensorId { get; set; }
        public string TimeStamp { get; set; }
        public double Temperature { get; set; }
    }

    public class TemperatureHumidity_Data
    {
        public string SensorId { get; set; }
        public string TimeStamp { get; set; }
        public double Temperature { get; set; }
        public double Humidity { get; set; }
    }

    public class Inclination_Data
    {
        public string SensorId { get; set; }
        public string TimeStamp { get; set; }
        public double X { get; set; }
        public double Y { get; set; }
        public double DeltaX { get; set; }
        public double DeltaY { get; set; }
    }

    public class RabbitMsg
    {
        public string RouteKey { get; set; }
        public string Body { get; set; }
    }
}