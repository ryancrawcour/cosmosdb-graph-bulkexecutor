using Microsoft.Azure.CosmosDB.BulkExecutor.Graph.Element;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace GraphBulkImporter
{
    public static class Extension
    {
        /// <summary>
        /// Turns an object in to a GremlinVertex with id & label, and a property bag of all other properties
        /// If obj does not have a property named id and partitionKey property, or you want a custom label applied 
        /// use an overload that allows you to specify which property will be used as "id" and "partitionKey"
        /// as well as allowing you to set a custom label
        /// </summary>
        /// 
        /// <returns>
        /// A new GremlinVertex object 
        /// with its id and partitionKey properties set to the corresspinding id and partitionKey properties of the object being converted
        /// and the label property set to the type name of the object
        /// </returns>
        public static GremlinVertex ToGremlinVertex(this object obj)
        {
            var label = obj.GetType().Name;
            
            if (!obj.HasProperty("id"))
            {
                throw new ArgumentException($"{label} does not have expected property.", "id");
            }

            if (!obj.HasProperty("partitionKey"))
            {
                throw new ArgumentException($"{label} does not have expected property.", "partitionKey");
            }

            return ToGremlinVertex(obj, "id", "partitionKey", label);
        }

        /// <summary>
        /// Converts an object to a GremlinVertex allowing more control over how the id, partitionKey, and label properties are set. 
        /// </summary>
        /// <param name="idProperty">Which property of the object should be used for id of the GremlinVertex, defaults to "id"</param>
        /// <param name="partitionKeyProperty">Which property should be used for partitionKey of the GremlinVertex, defaults to "partitionKey"</param>
        /// <param name="label">What value should be used for the label property, defaults to the Type name of the object</param>
        /// <returns>
        /// A new instance of a GremlinVertex
        /// with its id and partitionKey property values set to and <typeparamref name="idProperty"/> and <typeparamref name="partitionKeyProeprty"/> properties respectively
        /// and the label property set to the value of <typeparamref name="label"/>
        /// </returns>
        public static GremlinVertex ToGremlinVertex(this object obj, string idProperty,  string partitionKeyProperty, string label)
        {
            if (string.IsNullOrEmpty(idProperty))
            {
                throw new ArgumentException($"{nameof(idProperty)} cannot be Null or Empty", nameof(idProperty));
            }

            if (string.IsNullOrWhiteSpace(partitionKeyProperty))
            {
                throw new ArgumentException($"{nameof(partitionKeyProperty)} cannot be Null or Empty", nameof(partitionKeyProperty));
            }

            if (string.IsNullOrWhiteSpace(label))
            {
                throw new ArgumentException($"{nameof(label)} cannot be Null or Empty", nameof(label));
            }

            var gv = new GremlinVertex(obj.GetPropertyValue(idProperty).ToString(), label);
            gv.AddProperty(new GremlinVertexProperty("partitionKey", obj.GetPropertyValue(partitionKeyProperty)));

            foreach (var prop in obj.GetType().GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance))
            {
                var name = prop.Name;
                var val = prop.GetValue(obj);

                if (!name.Equals("id", StringComparison.InvariantCultureIgnoreCase) && !name.Equals("partitionKey", StringComparison.InvariantCultureIgnoreCase))
                    gv.AddProperty(new GremlinVertexProperty(name, val));
            }

            return gv;
        }       
        private static bool HasProperty(this object obj, string propertyName)
        {
            return GetPropertyInfo(obj, propertyName) != null;
        }
        private static PropertyInfo GetPropertyInfo(object obj, string propertyName)
        {
            //do a case-insensitive search for the propertyName supplied
            //i.e. match on Id, ID, and on id
            return obj.GetType().GetProperty(propertyName, BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
        }
        private static object GetPropertyValue(this object obj, string propertyName)
        {
            var prop = GetPropertyInfo(obj, propertyName);
            if (prop == null) throw new ArgumentException($"No {propertyName} property found.", propertyName);

            return prop.GetValue(obj);
        }
    }
}
