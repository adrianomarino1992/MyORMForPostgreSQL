namespace MyORMForPostgreSQL.Tests 
{
    using Utils;
    public class ConnectionAndDDLTests
    {
        private Classes.Context Context { get; }

        private const string _schema = "orm_pg_test";

        private const string _datname = "orm_pg_test_db";

        public ConnectionAndDDLTests()
        {
            Context = new Classes.Context(
                new PGManager(
                    new PGConnectionBuilder("supervisor", "sup", 5434, "localhost", _datname, _schema)
                    )
                );                        
            
        }

        [Fact]
        public void TestConnection()
        {
            Context.TestConnection();
        }

        [Fact]
        public void CreateDataBase()
        {
            SQL.DropDatabase();

            Context.CreateDataBase();

            int dbExists = 0;

            bool canQuery = SQL.ExecuteScalar<int>($"select 1 from pg_database where datname ilike '{_datname}' ;", out dbExists);            

            Assert.True(canQuery);

            Assert.Equal(1, dbExists!);

            SQL.DropDatabase();

        }

        [Fact]
        public void DropDataBase()
        {
            SQL.DropDatabase();

            SQL.Execute($"create database {_datname} ;");

            int dbExists = 0;

            bool canQuery = SQL.ExecuteScalar<int>($"select 1 from pg_database where datname ilike '{_datname}' ;", out dbExists);

            Assert.True(canQuery);

            Assert.Equal(1, dbExists!);

            Context.DropDataBase();

            dbExists = 0;

            canQuery = SQL.ExecuteScalar<int>($"select 1 from pg_database where datname ilike '{_datname}' ;", out dbExists);

            Assert.False(false);

            Assert.Equal(0, dbExists!);

            SQL.DropDatabase();


        }

        [Fact]
        public void CreateColumns()
        {
            SQL.DropDatabase();

            Context.CreateDataBase();

            Context.UpdateDataBase();

            long tbCount = 0;

            bool canQuery = SQL.ExecuteScalar<long>($"select count(*) from information_schema.tables where table_schema = '{_schema}' ;", out tbCount, _datname);

            Assert.True(canQuery);

            Assert.Equal(Context.MappedTypes.Count(), tbCount!);


            long colsCount = 0;

            canQuery = SQL.ExecuteScalar<long>($"select count(*) from information_schema.columns where table_schema = '{_schema}' ;",  out colsCount,  _datname);

            Assert.True(canQuery);

            int colsInContext = Context.MappedTypes
               .SelectMany(s =>
                           s.GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance)
                                   .Where(s => s.PropertyType.IsValueType || s.PropertyType.Equals(typeof(string)) || s.PropertyType.IsAssignableTo(typeof(System.Collections.IEnumerable))))
                                   .Where(s => {

                                       if (!(s.PropertyType.IsValueType || s.PropertyType.Equals(typeof(string))))
                                       {
                                           if (s.PropertyType.IsAssignableTo(typeof(System.Collections.IEnumerable)))
                                           {
                                               var e = s.PropertyType.GetGenericArguments()[0];

                                               return e.IsValueType || e.Equals(typeof(string));
                                           }

                                       }
                                       return true;
                                   })
               .Count();

            Assert.Equal(colsInContext, colsCount!);

            SQL.DropDatabase();

        }



        [Fact]
        public void FitColumns()
        {
            SQL.DropDatabase();

            Context.CreateDataBase();

            Context.UpdateDataBase();

            long tbCount = 0;

            bool canQuery = SQL.ExecuteScalar<long>($"select count(*) from information_schema.tables where table_schema = '{_schema}' ;", out tbCount, _datname);

            Assert.True(canQuery);

            Assert.Equal(Context.MappedTypes.Count(), tbCount!);

            SQL.Execute($"alter table {_schema}.departament add column test text ;", _datname);

            long colsCount = 0;

            int colsInContext = Context.MappedTypes
               .SelectMany(s =>
                           s.GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance)
                                   .Where(s => s.PropertyType.IsValueType || s.PropertyType.Equals(typeof(string)) || s.PropertyType.IsAssignableTo(typeof(System.Collections.IEnumerable))))
                                   .Where(s => { 
                                        
                                       if(!(s.PropertyType.IsValueType || s.PropertyType.Equals(typeof(string))))
                                       {
                                           if(s.PropertyType.IsAssignableTo(typeof(System.Collections.IEnumerable)))
                                           {
                                               var e = s.PropertyType.GetGenericArguments()[0];

                                               return e.IsValueType || e.Equals(typeof(string));
                                           }

                                       }
                                       return true;
                                   })
               .Count();

            canQuery = SQL.ExecuteScalar<long>($"select count(*) from information_schema.columns where table_schema = '{_schema}' ;", out colsCount, _datname);

            Assert.True(canQuery);

            Assert.Equal((colsInContext + 1), colsCount);

            Context.UpdateDataBase();

            colsCount = 0;

            canQuery = SQL.ExecuteScalar<long>($"select count(*) from information_schema.columns where table_schema = '{_schema}' ;", out colsCount, _datname);

            Assert.True(canQuery);          

            Assert.Equal(colsInContext, colsCount!);

            SQL.DropDatabase();

        }
    }

}
