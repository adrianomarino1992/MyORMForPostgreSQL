using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MyORM.Attributes;

namespace MyORMForPostgreSQL.Tests.Classes
{
    public class Context : PGContext
    {
        public Context(PGManager manager) : base(manager)
        {
             
        }

        public PGCollection<Seller> Sellers { get; set; }
        public PGCollection<Departament> Departaments { get; set; }
        public PGCollection<Sale> Sales { get; set; }
        public PGCollection<Product> Products { get; set; }

    }


    public class Seller : Entity
    {
        public string Name { get; set; }

        public string Email { get; set; }

        public Departament Departament { get; set; }

        [DBForeignKey]
        public long DepartamentId { get; set; }

        public List<Sale> Sales { get; set; }

        public List<string> Phones { get; set; } 

    }



    public class Departament : Entity
    {
        public string Name { get; set; }
    }

    public class Sale : Entity
    { 
        public int Quantity { get; set; }

        public Product Product { get; set; }

        [DBForeignKey]
        public long ProductId { get; set; }

        public Seller Seller { get; set; }

        [DBForeignKey]
        public long SellerId { get; set; }
    }


    public class Product : Entity
    {
        public string Name { get; set; }

        public double Value { get; set; }
    }

    public abstract class Entity
    {
        [DBPrimaryKey]
        public long Id { get; set; }
    }

}
