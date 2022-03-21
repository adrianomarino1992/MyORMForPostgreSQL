using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MyORM.Enums;
using MyORM.Interfaces;

using Npgsql;

namespace MyORMForPostgreSQL.Objects
{
    public class PGConnectionBuilder : IDBConnectionBuilder
    {
        public PGConnectionBuilder(string user, string password, int port = 5432, string host = "127.0.0.1", string dataBase = "postgres", string schema = "public", ProviderType providerType = ProviderType.POSTGRESQL)
        {
            User = user;
            Password = password;
            Port = port;
            Host = host;
            DataBase = dataBase;
            Schema = schema;
            ProviderType = providerType;
        }

        public string User { get; set; }
        public string Password { get; set; }
        public int Port { get; set; }
        public string Host { get; set; }
        public string DataBase { get; set; }
        public string Schema { get; set; }
        public ProviderType ProviderType { get; set; }

        public IDbCommand NewCommand(IDbConnection conn)
        {
            return new Npgsql.NpgsqlCommand("", conn as Npgsql.NpgsqlConnection);
        }

        public IDbConnection NewConnection()
        {
            return new Npgsql.NpgsqlConnection($"User ID={User};Password={Password};Host={Host};Port={Port};Database={DataBase}");
        }
    }
}
