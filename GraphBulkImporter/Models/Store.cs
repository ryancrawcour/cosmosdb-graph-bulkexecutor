using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GraphBulkImporter.Models
{
    public sealed class Point
    {
        public Point(Position position)
        {
            if (position == null)
            {
                throw new ArgumentNullException("position");
            }

            this.coordinates = position;
        }

        public readonly string type = "Point";
        public Position coordinates { get; private set; }
    }
    public class Position
    {
        internal double longitude { get; set; }
        internal double latitude { get; set; }

        internal Position(double longitude, double latitude)
        {
            this.longitude = longitude;
            this.latitude = latitude;
        }
    }

    class Store
    {
        public int id { get { return this.storeNbr; } }
        public string cbsAName { get; set; }
        public string city { get; set; }
        public string county { get; set; }
        public short countyType { get; set; }
        public Boolean isActive { get; set; }
        public string stateCode { get; set; }
        public string state { get; set; }
        public Point location { get; set; }
        public string name { get; set; }
        public int storeNbr { get; set; }
        public string storeStatus { get; set; }
        public string storeType { get; set; }
    }
}
