using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MyORMForPostgreSQL.Tests.Classes;
using MyORMForPostgreSQL.Tests.Utils;
using MyORM.Linq;

namespace MyORMForPostgreSQL.Tests
{
    
    public class QuerysTests
    {
        private Classes.Context Context { get; }

        private const string _schema = "orm_pg_test";

        private const string _datname = "orm_pg_test_db";

        public QuerysTests()
        {
            Context = new Classes.Context(
                new PGManager(
                    new PGConnectionBuilder("supervisor", "sup", 5434, "localhost",_datname, _schema)
                    )
                );

            Context.DropDataBase();

        }


        [Fact]
        public void TestQueryMethodWithString()
        {
            _CreateAndUpdate();

            Seller s1 = _GetSeller();
            Seller s2 = _GetSeller();

            s2.Name = "Seller 2";

            Context.Sellers.Add(s1);
            Context.Sellers.Add(s2);


            Seller result = Context.Sellers.Where(s => s.Name == s2.Name).First();

            Assert.Equal(s2.Name, result.Name);

            _Drop();
        }


        [Fact]
        public void TestQueryMethodWithInt()
        {
            _CreateAndUpdate();

            Seller s1 = _GetSeller();
            Seller s2 = _GetSeller();

            s2.Name = "Seller 2";

            Context.Sellers.Add(s1);
            Context.Sellers.Add(s2);


            Seller result = Context.Sellers.Where(s => s.Id == s2.Id).First();

            Assert.Equal(s2.Name, result.Name);

            _Drop();
        }


        [Fact]
        public void TestOrderBy()
        {
            _CreateAndUpdate();

            Seller s1 = _GetSeller();
            Seller s2 = _GetSeller();

            s1.Name = "Seller 3";
            s2.Name = "Seller 1";

            Context.Sellers.Add(s1);
            Context.Sellers.Add(s2);


            var result = Context.Sellers.OrderBy(s => s.Name).ToList();

            Assert.Equal(s2.Name, result[0].Name);
            Assert.Equal(s1.Name, result[1].Name);

            _Drop();
        }


        [Fact]
        public void TestLimit()
        {
            _CreateAndUpdate();

            Seller s1 = _GetSeller();
            Seller s2 = _GetSeller();

            Context.Sellers.Add(s1);
            Context.Sellers.Add(s2);


            var result = Context.Sellers.Limit(1).ToList();

            Assert.Single(result);          

            _Drop();
        }


        private void _Drop() => Context.DropDataBase();

        private void _CreateAndUpdate()
        {
            Context.DropDataBase();

            Context.UpdateDataBase();
        }

        private Seller _GetSeller()
        {
            Seller control = new Seller();

            control.Phones = new List<string>
            {
             "12 1234 5678",
             "23 4567 8910"
            };

            control.Name = "SellerName";
            control.Departament = new Departament()
            {
                Name = "Test"
            };
            control.Sales = new List<Sale>
            {
                new Sale
                {
                    Product = new Product
                    {
                        Name = "Procuct",
                        Value = 10.5
                    }
                },
                new Sale
                {
                    Product = new Product{
                        Name = "Product 2",
                        Value = 12.5
                    }
                }
            };

            return control;
        }
    }
}
