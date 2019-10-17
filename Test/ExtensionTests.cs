using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;

using CosmosDB.Graph.Extensions;
using Microsoft.Azure.CosmosDB.BulkExecutor.Graph.Element;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;


namespace Test
{
    [TestClass]
    public class ToGremlinVertexTests
    {
        [TestMethod]
        [ExpectedException(typeof(MissingFieldException))]
        public void NoIdNoPK()
        {
            new { FirstName = "Joe", LastName = "Franks" }.ToGremlinVertex();
        }

        [TestMethod]
        [ExpectedException(typeof(MissingFieldException))]
        public void SetIdNoPK()
        {
            new { Id = "1234567890" }.ToGremlinVertex();
        }

        [TestMethod]
        [ExpectedException(typeof(MissingFieldException))]
        public void SetPKNoId()
        {
            new { PartitionKey = "1" }.ToGremlinVertex();
        }

        [ExpectedException(typeof(MissingFieldException))]
        [DataTestMethod]
        [DataRow("Id", "")]
        [DataRow("", "partitionKey")]
        // test all case variations of id, Id, and ID
        public void SetEmptyIdAndPk(string idVal, string pkVal)
        {
            new { Id =idVal, LastName = pkVal}.ToGremlinVertex();
        }

        [TestMethod]
        // test all case variations of id, Id, and ID
        public void SetIdPropertyCaseVariations()
        {
            const string idval = "12345";
            const string pkval = "pk";

            //lowercase id
            var gv = new { id = idval, partitionKey = pkval }.ToGremlinVertex();
            Assert.AreEqual(idval, gv.Id);

            //Id
            gv = new { Id = idval, partitionKey = pkval }.ToGremlinVertex();
            Assert.AreEqual(idval, gv.Id);

            //uppercase ID
            gv = new { ID = idval, PARTITIONKEY = pkval }.ToGremlinVertex();
            Assert.AreEqual(idval, gv.Id);
        }

        [TestMethod]
        // test all case variations of partitionkey, partitionKey, and PartitionKey, PARTITIONKEY
        public void SetPkPropertyCaseVariations()
        {
            const string idval = "id";
            const string pkval = "pk value";

            var gv = new { id = idval, partitionkey = pkval }.ToGremlinVertex();
            Assert.AreEqual(pkval, gv.GetVertexProperties("partitionKey").FirstOrDefault().Value);

            gv = new { Id = idval, partitionKey = pkval }.ToGremlinVertex();
            Assert.AreEqual(pkval, gv.GetVertexProperties("partitionKey").FirstOrDefault().Value);

            gv = new { ID = idval, PartitionKey = pkval }.ToGremlinVertex();
            Assert.AreEqual(pkval, gv.GetVertexProperties("partitionKey").FirstOrDefault().Value);

            gv = new { ID = idval, PARTITIONKEY = pkval }.ToGremlinVertex();
            Assert.AreEqual(pkval, gv.GetVertexProperties("partitionKey").FirstOrDefault().Value);
        }

        [TestMethod]
        //we expect the GremlinVertex Label property to be the type name of the object we're converting
        public void NoLabelSet()
        {
            var obj = new { id = "id", partitionKey = "pk" };
            var labelval = obj.GetType().Name;

            var gv = obj.ToGremlinVertex();
            Assert.AreEqual(labelval, gv.Label);
        }

        [TestMethod]
        public void IdAndPkExistsNoLabel()
        {
            var idval = "id";
            var pkval = "pk";
            var nbr = 1;
            var str = "string";
            var dte = DateTime.UtcNow;

            var obj = new { id = idval, partitionKey = pkval, nbr, str, dte, NestedProp = new { someprop = "someval" } };
            var labelval = obj.GetType().Name;

            var gv = obj.ToGremlinVertex();
            Assert.AreEqual(idval, gv.Id);
            Assert.AreEqual(labelval, gv.Label);

            var props = gv.GetVertexProperties();

            Assert.IsTrue(props.Count<GremlinVertexProperty>() == 5);
            Assert.AreEqual("partitionKey", props.FirstOrDefault().Key);
            Assert.AreEqual(pkval, props.FirstOrDefault().Value);
            Assert.AreEqual(nbr, props.ElementAt(1).Value);
            Assert.AreEqual(str, props.ElementAt(2).Value);
            Assert.AreEqual(dte, props.ElementAt(3).Value);

            var nestedProp = props.ElementAt(4).Value;
            Assert.AreEqual("someval", nestedProp.GetType().GetProperty("someprop").GetValue(nestedProp));
        }

        [TestMethod]
        public void SetsIdPkAndLabel()
        {
            var obj = new { Name = "person", Age = 10 };

            //use the Name prop of the Person as the Id value
            //use the Age prop of the Person as the value for the partitionKey GremlinVertexProperty
            var gv = obj.ToGremlinVertex(idProperty: "Name", partitionKeyProperty: "Age", vertexLabel: "label");

            Assert.AreEqual(obj.Name, gv.Id);
            Assert.AreEqual("label", gv.Label);
            Assert.AreEqual(obj.Age, gv.GetVertexProperties("partitionKey").FirstOrDefault().Value);
            Assert.IsTrue(gv.GetVertexProperties().Count<GremlinVertexProperty>() == 3);
        }

        [TestMethod]
        public void BuildsFromNewtonsoftDeserializedObject()
        {
            const string idval = "id";
            const string pkval = "pk value";

            string serializedObject = JsonConvert.SerializeObject(new { Id = idval, partitionKey = pkval });
            dynamic deserializedObject = JsonConvert.DeserializeObject(serializedObject);

            GremlinVertex gv = ((object)deserializedObject).ToGremlinVertex();
            Assert.AreEqual(idval, gv.Id);
            Assert.AreEqual(pkval, gv.GetVertexProperties("partitionKey").FirstOrDefault().Value.ToString());
        }

        [DataTestMethod]
        [DataRow("Id", "PartitionKey")]
        [DataRow("id", "partitionkey")]
        [DataRow("iD", "partitionKey")]
        [DataRow("ID", "PARTITIONKEY")]
        public void BuildsFromExpandoObjectWithDifferentCasingVariations(string idPropertyName, string partitionKeyPropertyName)
        {
            const string idValue = "id";
            const string primaryKeyValue = "pk value";

            dynamic dynamicObject = new ExpandoObject();
            ((IDictionary<string, object>) dynamicObject).Add(idPropertyName, idValue);
            ((IDictionary<string, object>) dynamicObject).Add(partitionKeyPropertyName, primaryKeyValue);

            GremlinVertex gv = ((object) dynamicObject).ToGremlinVertex();
            Assert.AreEqual(idValue, gv.Id);
            Assert.AreEqual(primaryKeyValue, gv.GetVertexProperties("partitionKey").FirstOrDefault().Value.ToString());
        }
    }
}