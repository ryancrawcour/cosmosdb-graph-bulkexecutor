using System;
using System.Linq;

using CosmosDB.Graph.Extensions;
using Microsoft.Azure.CosmosDB.BulkExecutor.Graph.Element;
using Microsoft.VisualStudio.TestTools.UnitTesting;


namespace Test
{
    [TestClass]
    public class ToGremlinVertexTests
    {
        [TestMethod]
        [ExpectedException(typeof(MissingFieldException))]
        public void NoId_NoPK()
        {
            new { FirstName = "Joe", LastName = "Franks" }.ToGremlinVertex();
        }

        [TestMethod]
        [ExpectedException(typeof(MissingFieldException))]
        public void SetId_NoPK()
        {
            new { Id = "1234567890" }.ToGremlinVertex();
        }

        [TestMethod]
        [ExpectedException(typeof(MissingFieldException))]
        public void SetPK_NoId()
        {
            new { PartitionKey = "1" }.ToGremlinVertex();
        }

        [TestMethod]
        // test all case variations of id, Id, and ID
        public void CaseInsensitiveIdProperty()
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
        public void CaseInsensitivePkProperty()
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
        public void NoLabel_SetToTypeName()
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
        public void SetIdPkAndLabel()
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
    }
}