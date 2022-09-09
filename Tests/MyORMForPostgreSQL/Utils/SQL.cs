using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MyORMForPostgreSQL.Tests.Utils
{
    public static class SQL
    {
        public static void Execute(string sql, string database = "postgres")
        {
            NpgsqlConnection conn = null;
            try
            {
                conn = new NpgsqlConnection($"User ID=supervisor;Password=sup;Host=localhost;Port=5434;Database={database}");

                conn.Open();

                Npgsql.NpgsqlCommand cmmd = new NpgsqlCommand(sql, conn);

                cmmd.ExecuteNonQuery();
            }
            catch
            {

            }
            finally
            {
                conn.Close();
            }
        }

        public static bool ExecuteScalar<T>(string sql, out T @out, string database = "postgres")
        {
            NpgsqlConnection conn = null;
            try
            {
                conn = new NpgsqlConnection($"User ID=supervisor;Password=sup;Host=localhost;Port=5434;Database={database}");

                conn.Open();

                Npgsql.NpgsqlCommand cmmd = new NpgsqlCommand(sql, conn);

                @out = (T)cmmd.ExecuteScalar()!;

                return true;
            }
            catch
            {

            }
            finally
            {
                conn!.Close();
            }

            @out = default(T);

            return false;
        }

        public static void DropDatabase()
        {
            if (SQL.ExecuteScalar<int>($"select 1 from pg_database where datname ilike 'orm_pg_test_db' ;", out int i) && i == 1)
            {
                SQL.Execute("drop database orm_pg_test_db ;");
            }
        }
    }
}
