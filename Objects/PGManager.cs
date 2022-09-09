using System.Reflection;
using System.Collections;
using System.Data;

using MyORM.Attributes;
using MyORM.Exceptions;
using MyORM.Interfaces;
using MyORM.Enums;


namespace MyORMForPostgreSQL.Objects
{

    public class PGManager : IDBManager
    {
        public PGConnectionBuilder PGConnectionBuilder { get; }

        public PGManager(PGConnectionBuilder builder)
        {
            PGConnectionBuilder = builder;
        }

        public void CreateColumn(string table, PropertyInfo info)
        {

            bool createColumn = info.GetCustomAttribute<DBIgnoreAttribute>() == null;

            if (!createColumn)
                return;

            bool primaryKey = info.GetCustomAttribute<DBPrimaryKeyAttribute>() != null;


            if (primaryKey && info.PropertyType != typeof(long))
                throw new InvalidTypeException($"The type of a primary key must be {typeof(long).Name}");


            bool foreignKey = info.GetCustomAttribute<DBForeignKeyAttribute>() != null;

            if (foreignKey && info.PropertyType != typeof(long))
                throw new InvalidTypeException($"The type of a foreign key must be {typeof(long).Name}");


            bool isArray = info.PropertyType.IsAssignableTo(typeof(IEnumerable)) && info.PropertyType != typeof(string);

            string colName, colType = String.Empty;
                        

            if(isArray)
            {

                Type arrayType = info.PropertyType.GetElementType() ?? info.PropertyType.GetGenericArguments()[0];

                bool isValueType = (!arrayType.IsClass) || arrayType == typeof(string) ;

                if(isValueType)
                {
                    try
                    {
                        colName = info.GetCustomAttribute<DBColumnAttribute>()?.Name ?? info.Name.ToLower();
                        colType = GetDBTypeFromStruct(arrayType);
                    }
                    catch
                    {
                        return;
                    }

                    if (ExecuteScalar<int>($"SELECT 1 FROM information_schema.columns WHERE table_catalog = '{PGConnectionBuilder.DataBase}' AND table_name = '{table}' AND column_name = '{colName}'") == 1)
                        return;
                    ExecuteScalar<int>($"ALTER TABLE {PGConnectionBuilder.Schema}.{table} ADD COLUMN {colName} {colType}[] ");
                        return;
                }                

            }

            try
            {
                (colName, colType) = GetColumnNameAndType(info);
            }
            catch
            {
                return;
            }


            primaryKey = primaryKey && (colType.Trim() == "integer" || colType.Trim() == "bitint" || colType.Trim() == "serial" || colType.Trim() == "bigserial");

            if (ExecuteScalar<int>($"SELECT 1 FROM information_schema.columns WHERE table_catalog = '{PGConnectionBuilder.DataBase}' AND table_name = '{table}' AND column_name = '{colName}'") == 1)
                return;
            ExecuteScalar<int>($"ALTER TABLE {PGConnectionBuilder.Schema}.{table} ADD COLUMN {colName} {colType} {(primaryKey ? " NOT NULL PRIMARY KEY " : "")}");

            if (primaryKey)
                return;

            PropertyInfo? foreingKeyType = info.ReflectedType?.GetProperties(BindingFlags.Public | BindingFlags.Instance).FirstOrDefault(d => d.Name == info.Name.Replace("Id", ""));
            PropertyInfo? subKeyProperty = info.ReflectedType?.GetProperties(BindingFlags.Public | BindingFlags.Instance).FirstOrDefault(d => d.GetCustomAttribute<DBPrimaryKeyAttribute>() != null);

            if (subKeyProperty != null && foreingKeyType != null && foreignKey)
            {
                string subTable = foreingKeyType.PropertyType.GetCustomAttribute<DBTableAttribute>()?.Name ?? foreingKeyType.PropertyType.Name.ToLower();
                string subColName = subKeyProperty.GetCustomAttribute<DBColumnAttribute>()?.Name ?? subKeyProperty.Name.ToLower();
                DeleteMode mode = info.GetCustomAttribute<DBDeleteModeAttribute>()?.DeleteMode ?? DeleteMode.NOACTION;
                string consName = $"{table}_{colName}_fkey";
                string constraint = $@"ALTER TABLE {PGConnectionBuilder.Schema}.{table} ADD CONSTRAINT {consName} FOREIGN KEY ({colName})
                                        REFERENCES {PGConnectionBuilder.Schema}.{subTable} ({subColName}) 
                                        ON UPDATE NO ACTION
                                        ON DELETE {(mode == DeleteMode.CASCADE ? "CASCADE" : "NO ACTION")}";

                if (ExecuteScalar<int>($"SELECT 1 FROM information_schema.constraint_column_usage  WHERE table_catalog = '{PGConnectionBuilder.DataBase}' AND constraint_name  = '{table}' AND column_name = '{consName}'") == 1)
                    return;

                ExecuteNonQuery(constraint);

            }
        }

        public (string, string) GetColumnNameAndType(PropertyInfo info, bool checkKey = true)
        {
            string colName = info.GetCustomAttribute<DBColumnAttribute>()?.Name ?? info.Name.ToLower();
            bool key = info.GetCustomAttribute<DBPrimaryKeyAttribute>() != null;

            if (key && checkKey)
            {
                if (info.PropertyType == typeof(Int32))
                    return (colName, " serial ");
                if (info.PropertyType == typeof(long))
                    return (colName, " bigserial ");
            }                

            if (info.PropertyType == typeof(string))
                return (colName, " text ");

            if (info.PropertyType == typeof(Int32))
                return (colName, " integer ");

            if (info.PropertyType == typeof(long))
                return (colName, " bigint ");

            if (info.PropertyType == typeof(double) || info.PropertyType == typeof(float))
                return (colName, " real ");

            if (info.PropertyType == typeof(DateTime))
                return (colName, " date ");

            if (info.PropertyType == typeof(bool))
                return (colName, " boolean ");

            if (info.PropertyType.IsEnum)
                return (colName, " integer ");

            throw new CastFailException($"Can not cast the property {info.Name} to a column");

        }


        public string GetDBTypeFromStruct(Type type)
        {
            if (type == typeof(string))
                return " text ";

            if (type == typeof(Int32))
                return  " integer ";

            if (type == typeof(long))
                return " bigint ";

            if (type == typeof(double) || type == typeof(float))
                return " real ";

            if (type == typeof(DateTime))
                return " date ";

            if (type == typeof(bool))
                return " boolean ";

            if (type.IsEnum)
                return " integer ";

            throw new CastFailException($"Can not cast the type {type.Name} to a DBType");


        }

        public void CreateDataBase()
        {
            if (!DataBaseExists())
            {
                ExecuteNonQuery($"CREATE DATABASE {PGConnectionBuilder.DataBase.ToLower().Trim()}", DB.POSTGRESQL);
            }

            ExecuteNonQuery($"CREATE SCHEMA IF NOT EXISTS {PGConnectionBuilder.Schema.ToLower().Trim()}", DB.BUILDER);


        }

        public void CreateTable<T>()
        {

            string tableName = typeof(T).GetCustomAttribute<DBTableAttribute>()?.Name ?? typeof(T).Name.ToLower();

            ExecuteNonQuery($"CREATE TABLE IF NOT EXISTS {PGConnectionBuilder.Schema}.{tableName}()");           

        }

        public bool ColumnExists(string table, string colName)
        {
            return ExecuteScalar<int>($"SELECT * FROM information_schema.columns WHERE table_catalog = '{PGConnectionBuilder.DataBase}' AND table_name = '{table}' AND column_name = '{colName}'") == 1;
        }

        public bool DataBaseExists()
        {
            return ExecuteScalar<int>($"SELECT 1 FROM pg_database WHERE datname='{PGConnectionBuilder.DataBase.ToLower().Trim()}'", DB.POSTGRESQL) == 1;
        }


        public bool TableExists<T>()
        {
            string tableName = typeof(T).GetCustomAttribute<DBTableAttribute>()?.Name ?? typeof(T).Name.ToLower();

            return ExecuteScalar<int>($"SELECT * FROM information_schema.tables WHERE table_catalog = '{PGConnectionBuilder.DataBase}' AND table_name = '{tableName}'") == 1;
        }


        public void DropColumn(string table, PropertyInfo info)
        {
            string colName = info.GetCustomAttribute<DBColumnAttribute>()?.Name ?? info.Name.ToLower();

            if (ColumnExists(table, colName))
            {
                ExecuteNonQuery($"ALTER TABLE {table} DROP COLUMN {colName}");
            }
        }

        public void DropDataBase()
        {
            if (DataBaseExists())
            {
                ExecuteNonQuery($"SELECT PG_TERMINATE_BACKEND(PID) FROM PG_STAT_ACTIVITY WHERE DATNAME = '{PGConnectionBuilder.DataBase.ToLower().Trim()}' ", DB.POSTGRESQL);
                ExecuteNonQuery($"DROP DATABASE {PGConnectionBuilder.DataBase.ToLower().Trim()}", DB.POSTGRESQL);
            }
        }

        public void DropTable<T>()
        {
            string tableName = typeof(T).GetCustomAttribute<DBTableAttribute>()?.Name ?? typeof(T).Name.ToLower();

            ExecuteNonQuery($"DROP TABLE IF EXISTS {tableName}");
        }

        public void FitColumns(string table, IEnumerable<PropertyInfo> infos)
        {
            DataSet? dt = GetDataSet($"SELECT column_name, data_type FROM information_schema.columns WHERE table_catalog = '{PGConnectionBuilder.DataBase}' AND table_name = '{table}'");

            List<(string?, string?)> columns = new List<(string?, string?)>();
            

            if (dt == null)
                return;

            if (dt.Tables.Count == 0 || dt.Tables[0].Rows.Count == 0)
                return;

            foreach (DataRow row in dt.Tables[0].Rows)
            {
                columns.Add((row["column_name"].ToString(), row["data_type"].ToString()));
            }

            foreach ((string? col, string? type) in columns)
            {
                PropertyInfo? info = infos.FirstOrDefault(d => d.GetCustomAttribute<DBColumnAttribute>()?.Name == col || (d.GetCustomAttribute<DBColumnAttribute>() == null && col == d.Name.ToLower()));
                if (col != null)
                {
                    if (info == null)
                    {
                        ExecuteNonQuery($"ALTER TABLE {PGConnectionBuilder.Schema}.{table} DROP COLUMN {col}");
                        continue;
                    }
                }
                

                if(info != null)
                {
                    bool isArray = info.PropertyType.IsAssignableTo(typeof(IEnumerable)) && info.PropertyType != typeof(string);

                    string colType = String.Empty;


                    if (isArray)
                    {

                        Type arrayType = info.PropertyType.GetElementType() ?? info.PropertyType.GetGenericArguments()[0];

                        bool isValueType = (!arrayType.IsClass) || arrayType == typeof(string);

                        if (isValueType)
                        {
                            try
                            {
                                colType = $"{GetDBTypeFromStruct(arrayType).Trim()}[]";

                                if (colType.Trim().ToLower() != type?.Trim().ToLower())
                                {
                                    try
                                    {
                                        ExecuteNonQuery($"ALTER TABLE {PGConnectionBuilder.Schema}.{table} ALTER COLUMN {col} TYPE {colType}");
                                    }
                                    catch
                                    {
                                        ExecuteNonQuery($"ALTER TABLE {PGConnectionBuilder.Schema}.{table} DROP COLUMN {col}");
                                        CreateColumn(table,info);

                                    }
                                    continue;
                                }
                                
                            }
                            catch
                            {
                                return;
                            }                            
                        }

                    }
                    else
                    {
                        (string _, colType) = GetColumnNameAndType(info, false);

                        if (colType.Trim().ToLower() != type?.Trim().ToLower())
                        {
                            try
                            {
                                ExecuteNonQuery($"ALTER TABLE {PGConnectionBuilder.Schema}.{table} ALTER COLUMN {col} TYPE {colType}");

                            }
                            catch
                            {
                                ExecuteNonQuery($"ALTER TABLE {PGConnectionBuilder.Schema}.{table} DROP COLUMN {col}");
                                CreateColumn(table, info);


                            }
                            continue;
                        }

                    }
                }

                
            }

        }

        public bool TryConnection()
        {
            IDbConnection conn = PGConnectionBuilder.NewConnection();

            try
            {
                conn.Open();
            }
            catch
            {

                return false;
            }
            finally
            {

                conn.Close();
            }

            return true;
        }

        public T? ExecuteScalar<T>(string query, DB db = DB.BUILDER)
        {
            string temp = String.Empty;

            if (db == DB.POSTGRESQL)
            {
                temp = PGConnectionBuilder.DataBase;
                PGConnectionBuilder.DataBase = "postgres";
            }

            IDbConnection conn = PGConnectionBuilder.NewConnection();

            if (db == DB.POSTGRESQL)
                PGConnectionBuilder.DataBase = temp;

            try
            {
                conn.Open();

                IDbCommand cmd = PGConnectionBuilder.NewCommand(conn);

                cmd.CommandText = query;

                object? r = cmd.ExecuteScalar();

                if (r == null)
                    return default(T);

                return (T)r;
            }
            catch (Exception ex)
            {
                throw new QueryFailException(ex.Message);
            }
            finally
            {
                conn.Close();
            }
        }

        public void ExecuteNonQuery(string query, DB db = DB.BUILDER)
        {
            string temp = String.Empty;

            if (db == DB.POSTGRESQL)
            {
                temp = PGConnectionBuilder.DataBase;
                PGConnectionBuilder.DataBase = "postgres";
            }

            IDbConnection conn = PGConnectionBuilder.NewConnection();

            if (db == DB.POSTGRESQL)
                PGConnectionBuilder.DataBase = temp;

            try
            {
                conn.Open();

                IDbCommand cmd = PGConnectionBuilder.NewCommand(conn);

                cmd.CommandText = query;

                cmd.ExecuteNonQuery();

            }
            catch (Exception ex)
            {
                throw new QueryFailException(ex.Message);
            }
            finally
            {
                conn.Close();
            }
        }

        public DataSet GetDataSet(string query, DB db = DB.BUILDER)
        {
            string temp = String.Empty;

            if (db == DB.POSTGRESQL)
            {
                temp = PGConnectionBuilder.DataBase;
                PGConnectionBuilder.DataBase = "postgres";
            }

            IDbConnection conn = PGConnectionBuilder.NewConnection();

            if (db == DB.POSTGRESQL)
                PGConnectionBuilder.DataBase = temp;

            try
            {
                conn.Open();

                IDbCommand cmd = PGConnectionBuilder.NewCommand(conn);

                cmd.CommandText = query;

#pragma warning disable CS8604 // Possível argumento de referência nula.
                IDataAdapter? r = new Npgsql.NpgsqlDataAdapter(cmd as Npgsql.NpgsqlCommand);
#pragma warning restore CS8604 // Possível argumento de referência nula.

                DataSet ds = new DataSet();

                r.Fill(ds);

                return ds;

            }
            catch (Exception ex)
            {
                throw new CastFailException(ex.Message);
            }
            finally
            {
                conn.Close();
            }
        }


    }

    public enum DB
    {
        POSTGRESQL = 0,
        BUILDER = 1

    }

}
