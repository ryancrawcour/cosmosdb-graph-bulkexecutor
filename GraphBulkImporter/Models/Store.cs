using System;

namespace GraphBulkImporter.Models
{
    public sealed class Point
    {
        public Point(Position position)
        {
            this.coordinates = position ?? throw new ArgumentNullException(nameof(position));
        }

        public readonly string type = "Point";
        public Position coordinates { get; private set; }
    }
    public sealed class Position
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
        public int Id { get { return this.StoreNbr; } }
        public string CBSName { get; set; }
        public string City { get; set; }
        public string County { get; set; }
        public short CountyType { get; set; }
        public Boolean IsActive { get; set; }
        public string StateCode { get; set; }
        public string State { get; set; }
        public Point Location { get; set; }
        public string Name { get; set; }
        public int StoreNbr { get; set; }
        public string StoreStatus { get; set; }
        public string StoreType { get; set; }
    }
}
