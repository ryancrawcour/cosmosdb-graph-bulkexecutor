using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using Microsoft.Azure.CosmosDB.BulkExecutor.Graph.Element;
using Microsoft.CSharp.RuntimeBinder;

namespace CosmosDB.Graph.Extensions
{
    public static class Extensions
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
            if (obj is IDynamicMetaObjectProvider provider)
            {
                return ToGremlinVertex(provider);
            }
            string typeName = obj.GetType().Name;
            
            if (!obj.HasProperty("id"))
            {
                throw new MissingFieldException($"{typeName} does not have expected property.", "id");
            }

            if (!obj.HasProperty("partitionKey"))
            {
                throw new MissingFieldException($"{typeName} does not have expected property.", "partitionKey");
            }

            //GremlineVertex has a Label property that is required. So GremlinVertex.Label == typeName.
            return ToGremlinVertex(obj, "id", "partitionKey", typeName);
        }

        public static GremlinVertex ToGremlinVertex(this IDynamicMetaObjectProvider obj)
        {
            Type scope = obj.GetType();
            ParameterExpression param = Expression.Parameter(scope);
            DynamicMetaObject metaObject = obj.GetMetaObject(param);
            IEnumerable<string> dynamicMemberNames = metaObject.GetDynamicMemberNames();

            string vertexLabel = scope.Name;
            string id = null;
            object partitionKey = null;
            List<Tuple<string, object>> vertexProperties = new List<Tuple<string, object>>();
            foreach (string memberName in dynamicMemberNames)
            {
                object value = ReadDynamicValue(obj, scope, metaObject, param, memberName);
                if (memberName.Equals("id", StringComparison.OrdinalIgnoreCase))
                {
                    id = value.ToString();
                }
                else if (memberName.Equals("partitionkey", StringComparison.OrdinalIgnoreCase))
                {
                    partitionKey = value;
                }
                else
                {
                    vertexProperties.Add(new Tuple<string, object>(memberName, value));
                }
            }

            if (id == null)
            {
                throw new MissingFieldException($"{vertexLabel} does not have expected property.", "id");
            }

            if (partitionKey == null)
            {
                throw new MissingFieldException($"{vertexLabel} does not have expected property.", "partitionKey");
            }

            return BuildGremlinVertex(vertexLabel, id, partitionKey, vertexProperties);
        }

        private static object ReadDynamicValue(IDynamicMetaObjectProvider obj, Type scope,
            DynamicMetaObject metaObject, ParameterExpression param, string propertyName)
        {
            GetMemberBinder binder = (GetMemberBinder) Microsoft.CSharp.RuntimeBinder.Binder.GetMember(0, propertyName, scope,
                new[] {CSharpArgumentInfo.Create(0, null)});
            DynamicMetaObject ret = metaObject.BindGetMember(binder);
            BlockExpression final = Expression.Block(
                Expression.Label(CallSiteBinder.UpdateLabel),
                ret.Expression
            );
            LambdaExpression lambda = Expression.Lambda(final, param);
            Delegate del = lambda.Compile();
            object result = del.DynamicInvoke(obj);
            return result;
        }

        /// <summary>
        /// Converts an object to a GremlinVertex allowing more control over how the id, partitionKey, and label properties are set. 
        /// </summary>
        /// <param name="idProperty">Which property of the object should be used for id of the GremlinVertex, defaults to "id"</param>
        /// <param name="partitionKeyProperty">Which property should be used for partitionKey of the GremlinVertex, defaults to "partitionKey"</param>
        /// <param name="vertexLabel">What value should be used for the label property, defaults to the Type name of the object</param>
        /// <returns>
        /// A new instance of a GremlinVertex
        /// with its id and partitionKey property values set to and <typeparamref name="idProperty"/> and <typeparamref name="partitionKeyProperty"/> properties respectively
        /// and the label property set to the value of <typeparamref name="label"/>
        /// </returns>
        public static GremlinVertex ToGremlinVertex(this object obj, string idProperty,  string partitionKeyProperty, string vertexLabel)
        {
            if (string.IsNullOrWhiteSpace(idProperty))
            {
                throw new ArgumentException($"{nameof(idProperty)} cannot be Null or Empty", nameof(idProperty));
            }

            if (string.IsNullOrWhiteSpace(partitionKeyProperty))
            {
                throw new ArgumentException($"{nameof(partitionKeyProperty)} cannot be Null or Empty",
                    nameof(partitionKeyProperty));
            }

            //Get a list of all Properties, except where the name is "id" or "partitionKey"
            IEnumerable<Tuple<string, object>> props = obj.GetType().GetProperties()
                .Where(x => !x.Name.Equals("id", StringComparison.InvariantCultureIgnoreCase)
                            && !x.Name.Equals("partitionKey", StringComparison.InvariantCultureIgnoreCase))
                .Select(item => new Tuple<string, object>(item.Name, item.GetValue(obj)));

            string id = obj.GetPropertyValue(idProperty).ToString();
            object partitionKey = obj.GetPropertyValue(partitionKeyProperty);

            return BuildGremlinVertex(vertexLabel, id, partitionKey, props);
        }

        private static GremlinVertex BuildGremlinVertex(string vertexLabel, string id, object partitionKey, IEnumerable<Tuple<string,object>> vertexProperties)
        {
            if (string.IsNullOrWhiteSpace(vertexLabel)) 
                throw new ArgumentNullException($"{nameof(vertexLabel)} cannot be Null or Empty",nameof(vertexLabel));
            if (string.IsNullOrWhiteSpace(id)) 
                throw new ArgumentNullException($"{nameof(id)} cannot be Null or Empty", nameof(id));
            if (partitionKey == null) 
                throw new ArgumentNullException($"{nameof(partitionKey)} cannot be Null or Empty", nameof(partitionKey));
            
            GremlinVertex gv = new GremlinVertex(id, vertexLabel);
            gv.AddProperty(new GremlinVertexProperty("partitionKey", partitionKey));
            
            foreach ((string name, object value) in vertexProperties)
            {
                gv.AddProperty(new GremlinVertexProperty(name, value));
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
            var prop = GetPropertyInfo(obj, propertyName)??throw new MissingFieldException($"No {propertyName} property found.", propertyName);

            return prop.GetValue(obj);
        }
    }
}
