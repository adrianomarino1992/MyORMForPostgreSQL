using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Linq.Expressions;
using System.Reflection;
using System.Data;

using MyORM.Interfaces;
using MyORM.Attributes;
using MyORM.Exceptions;
using static MyORMForPostgreSQL.Helpers.PGExpressionsManager;
using System.Collections;

namespace MyORMForPostgreSQL.Objects
{
    public class PGCollection<T> : IEntityCollection<T> where T : class
    {
        PGContext _context;

        protected string _sql = String.Empty;

        protected List<(PropertyInfo, string)> _subTypes;

        protected PGManager _pGManager;

#pragma warning disable //sql is generated in runtime
        public PGCollection(PGManager manager, PGContext context)
        {
#pragma warning disable

            _context = context;
            _pGManager = manager;
            _subTypes = new List<(PropertyInfo, string)>();
        }

        public ICommand GetCommand()
        {
            return new PGCommand(_sql);
        }

        public IQueryableCollection<T> Query<TResult>(Expression<Func<T, TResult>> expression)
        {
            PGCollection<T> result = new PGCollection<T>(_pGManager, _context);

            IEnumerable<(ExpressionType?, Expression?)> exs = SplitInBinaryExpressions(null, expression.Body);

            _sql = String.Empty;

            _subTypes = new List<(PropertyInfo, string)>();

            if (exs.Count() == 0)
                return this;

            if (_sql.Trim().Length == 0)
            {
                _sql += " WHERE ";
            }

            if (_sql.Trim().Length > 0 && !_sql.Trim().StartsWith("WHERE"))
            {
                _sql = " WHERE " + _sql.Trim();
            }

            if (_sql.Trim().Length > 0)
            {

                _sql += $"{(_sql.Trim().Equals("WHERE") ? " " : " AND ")}{ManageBinaryExpressions<T>(exs)} ";
            }

            result._sql = _sql;

            return result;
        }

        public IQueryableCollection<T> And<TResult>(Expression<Func<T, TResult>> expression)
        {
            PGCollection<T> result = new PGCollection<T>(_pGManager, _context);           

            IEnumerable<(ExpressionType?, Expression?)> exs = SplitInBinaryExpressions(null, expression.Body);


            if (_sql.Trim().Length > 0)
            {
                _sql = $" WHERE ( {_sql.Replace("WHERE", "")} ) AND {ManageBinaryExpressions<T>(exs)} ";
            }
            else
            {
                _sql = $" WHERE {ManageBinaryExpressions<T>(exs)} ";
            }

            result._sql = _sql;

            return result;
        }

        public IQueryableCollection<T> Or<TResult>(Expression<Func<T, TResult>> expression)
        {
            PGCollection<T> result = new PGCollection<T>(_pGManager, _context);            

            IEnumerable<(ExpressionType?, Expression?)> exs = SplitInBinaryExpressions(null, expression.Body);

            if (_sql.Trim().Length > 0)
            {
                _sql = $" WHERE ( {_sql.Replace("WHERE", "")} ) OR {ManageBinaryExpressions<T>(exs)} ";
            }
            else
            {
                _sql = $" WHERE {ManageBinaryExpressions<T>(exs)} ";
            }

            result._sql = _sql;

            return result;
        }

        public IQueryableCollection<T> OrderBy<TResult>(Expression<Func<T, TResult>> expression)
        {
            PGCollection<T> result = new PGCollection<T>(_pGManager, _context);

            MemberExpression? lambdaExpression = expression.Body as MemberExpression;

            if (lambdaExpression == null)
            {
                return this;
            }

            PropertyInfo? member = lambdaExpression.Member as PropertyInfo;

            if (member == null)
            {
                return this;
            }

            if
                (
                    (!(member.ReflectedType == typeof(T)))
                    && member.ReflectedType != null && !typeof(T).IsSubclassOf(member.ReflectedType)
                )
            {
                return this;
            }

            DBColumnAttribute? colName = member.GetCustomAttribute<DBColumnAttribute>();

            _sql = $" {_sql} ORDER BY {(String.IsNullOrEmpty(colName?.Name) ? member.Name.ToLower().Trim() : colName.Name)} ";

            result._sql = _sql;
                        

            foreach ((PropertyInfo infoS, string sqlS) in _subTypes)
            {
                result._subTypes.Add(new(infoS, sqlS));
            }            


            return result;


        }

        public IQueryableCollection<T> Limit(int limit)
        {
            PGCollection<T> result = new PGCollection<T>(_pGManager, _context);

            _sql = $" {_sql} LIMIT {limit} ";

            result._sql = _sql;

            foreach ((PropertyInfo infoS, string sqlS) in _subTypes)
            {
                result._subTypes.Add(new(infoS, sqlS));
            }

            return result;
        }

        public IQueryableCollection<T> OffSet(int offSet)
        {
            PGCollection<T> result = new PGCollection<T>(_pGManager, _context);

            _sql = $" {_sql} OFFSET {offSet} ";

            result._sql = _sql;

            foreach ((PropertyInfo infoS, string sqlS) in _subTypes)
            {
                result._subTypes.Add(new(infoS, sqlS));
            }

            return result;
        }



        public async Task<int> CountAsync()
        {
            return await Task.Run(() => Count());
        }

        public int Count()
        {
            string tableName = typeof(T).GetCustomAttribute<DBColumnAttribute>()?.Name ?? typeof(T).Name;

            string schema = _pGManager.PGConnectionBuilder.Schema;

            string query = $" SELECT COUNT(*)::integer FROM {schema}.{tableName} ";

            return _pGManager.ExecuteScalar<int>(query);
        }

        public async Task<IEnumerable<T>> RunAsync()
        {
            return await Task.Run(() => Run());
        }


        public IEnumerable<T> Run()
        {
            string tableName = typeof(T).GetCustomAttribute<DBColumnAttribute>()?.Name ?? typeof(T).Name;
            string schema = _pGManager.PGConnectionBuilder.Schema;

            string query = $" SELECT * FROM {schema}.{tableName} " + _sql;

            IEnumerable<T> list = Run(query);

            if (_subTypes.Count > 0 && list.Count() > 0)
            {
                List<T> ts = list.ToList();

                foreach ((PropertyInfo info, string sql) in _subTypes)
                {
                    StringBuilder tQuery = new StringBuilder(sql);

                    PropertyInfo? foreignKey = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance)
                                    .Where(d => d.GetCustomAttribute<DBIgnoreAttribute>() == null)
                                    .Where(d => d.PropertyType.IsValueType)
                                    .Where(d => d.Name == $"{info.Name}Id").FirstOrDefault();

                    if (foreignKey == null)
                        continue;

                    for (int i = 0; i < ts.Count; i++)
                    {
                        if (i == 0)
                        {
                            tQuery.Append(" ( ");
                        }

                        tQuery.Append($" {foreignKey.GetValue(ts[i]).ToString()} ");

                        if (i < ts.Count - 1)
                        {
                            tQuery.Append(" , ");
                        }

                        if (i == ts.Count - 1)
                        {
                            tQuery.Append(" ) ");
                        }

                    }

                    Type tG = typeof(List<>);

                    IList sList = _BuildEnumerable(info.PropertyType, _pGManager.GetDataSet(tQuery.ToString())) ?? Activator.CreateInstance(tG.MakeGenericType(info.PropertyType)) as IList;

                    PropertyInfo? key = info.PropertyType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                                  .Where(d => d.GetCustomAttribute<DBIgnoreAttribute>() == null)
                                  .Where(d => d.GetCustomAttribute<DBPrimaryKeyAttribute>() != null)
                                  .Where(d => d.PropertyType.IsValueType)
                                  .FirstOrDefault();



                    foreach (T rO in ts)
                    {
                        foreach (var sO in sList)
                        {
                            if (foreignKey.GetValue(rO).ToString() == key.GetValue(sO).ToString())
                            {
                                info.SetValue(rO, sO);
                                break;
                            }
                        }
                    }
                }


                return ts;
            }

            return list;

        }


        public async Task<IEnumerable<T>> RunAsync(string sql)
        {
            return await Task.Run(() => Run(sql));
        }

        public IEnumerable<T> Run(string sql)
        {
            DataSet dt = _pGManager.GetDataSet(sql);

            return _BuildEnumerable(typeof(T), dt) as IEnumerable<T> ?? new List<T>();
        }


        private IList? _BuildEnumerable(Type type, DataSet dt)
        {
            IList? list = Activator.CreateInstance(typeof(List<>).MakeGenericType(type)) as IList;

            if (dt == null)
                return list;

            if (dt.Tables.Count == 0 || dt.Tables[0].Rows.Count == 0)
                return list;

            foreach (DataRow row in dt.Tables[0].Rows)
            {
                var it = Activator.CreateInstance(type);

                if (it == null)
                    continue;

                List<PropertyInfo> props = it.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance)
                    .Where(d => (d.PropertyType == typeof(string) || !d.PropertyType.IsClass))
                    .Where(d => d.GetCustomAttribute<DBIgnoreAttribute>() == null).ToList();


                foreach (PropertyInfo prop in props)
                {

                    if (prop.PropertyType.IsAssignableTo(typeof(IEnumerable)) && prop.PropertyType != typeof(string))
                    {
                        Type arrayType = prop.PropertyType.GetElementType() ?? prop.PropertyType.GetGenericArguments()[0];

                        bool isValueType = (!arrayType.IsClass) || arrayType == typeof(string);

                        if (!isValueType)
                            continue;
                    }

                    string colName = prop.GetCustomAttribute<DBColumnAttribute>()?.Name ?? prop.Name.ToLower();

                    {
                        if (prop.PropertyType == typeof(string) && prop.SetMethod != null)
                        {
                            prop.SetValue(it, row[colName].ToString());
                            continue;
                        }
                    }

                    {
                        if (prop.PropertyType == typeof(int) && prop.SetMethod != null && int.TryParse(row[colName].ToString(), out int _out))
                        {
                            prop.SetValue(it, _out);
                            continue;
                        }
                    }

                    {
                        if (prop.PropertyType == typeof(bool) && prop.SetMethod != null)
                        {
                            prop.SetValue(it, row[colName]);
                            continue;
                        }
                    }

                    {
                        if (prop.PropertyType.IsEnum && prop.SetMethod != null)
                        {

                            foreach (var enums in Enum.GetValues(prop.PropertyType))
                            {
                                if ((int)enums == (int)row[colName])
                                {
                                    prop.SetValue(it, row[colName]);
                                    goto END;
                                }

                            }
                        END:
                            continue;
                        }
                    }

                    {
                        if (prop.PropertyType == typeof(decimal) && prop.SetMethod != null && decimal.TryParse(row[colName].ToString(), out decimal _out))
                        {
                            prop.SetValue(it, _out);
                            continue;
                        }
                    }

                    {
                        if (prop.PropertyType == typeof(float) && prop.SetMethod != null && float.TryParse(row[colName].ToString(), out float _out))
                        {
                            prop.SetValue(it, _out);
                            continue;
                        }
                    }

                    {
                        if (prop.PropertyType == typeof(double) && prop.SetMethod != null && double.TryParse(row[colName].ToString(), out double _out))
                        {
                            prop.SetValue(it, _out);
                            continue;
                        }
                    }

                    {
                        if (prop.PropertyType == typeof(long) && prop.SetMethod != null && long.TryParse(row[colName].ToString(), out long _out))
                        {
                            prop.SetValue(it, _out);
                            continue;
                        }
                    }

                    {
                        if (prop.PropertyType == typeof(DateTime) && prop.SetMethod != null && DateTime.TryParse(row[colName].ToString(), out DateTime _out))
                        {
                            prop.SetValue(it, _out);
                            continue;
                        }
                    }

                    {
                        if (prop.PropertyType.IsAssignableTo(typeof(IEnumerable)) && prop.SetMethod != null)
                        {
                            object dbV = row[colName];

                            string s = dbV.ToString();
                            continue;

                        }
                    }
                }


                 props = it.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance)
                   .Where(d => (d.PropertyType != typeof(string) &&  d.PropertyType.IsAssignableTo(typeof(IEnumerable))))
                   .Where(d => d.GetCustomAttribute<DBIgnoreAttribute>() == null).ToList();


                foreach (PropertyInfo prop in props)
                {

                    Type arrayType = prop.PropertyType.GetElementType() ?? prop.PropertyType.GetGenericArguments()[0];

                    bool isValueType = (!arrayType.IsClass) || arrayType == typeof(string);

                    if (!isValueType)
                        continue;

                    string colName = prop.GetCustomAttribute<DBColumnAttribute>()?.Name ?? prop.Name.ToLower();


                    if (prop.PropertyType.GetElementType() != null)
                    {
                        object dbV = row[colName];

                        if(dbV != null && dbV is Array dbarr)
                        {
                            Array arr = Array.CreateInstance(prop.PropertyType.GetElementType(), dbarr.Length);

                            for (int i = 0; i < dbarr.Length; i++)
                            {
                                arr.SetValue(dbarr.GetValue(i), i);
                            }

                            prop.SetValue(it, arr);


                        }

                        continue;

                    }


                    if (prop.PropertyType.GetGenericArguments().Length > 0 && prop.PropertyType.GetGenericArguments()[0] != null)
                    {
                        object dbV = row[colName];

                        if (dbV != null && dbV is Array dbarr)
                        {
                            IList arr = (IList)Activator.CreateInstance(typeof(List<>).MakeGenericType(prop.PropertyType.GetGenericArguments()[0]));

                            for (int i = 0; i < dbarr.Length; i++)
                            {
                                arr.Add(dbarr.GetValue(i));
                            }

                            prop.SetValue(it, arr);


                        }

                        continue;

                    }
                }
                
                
                list?.Add(it);
            }

            return list;
        }


        public async Task<T> AddAsync(T obj)
        {
            return await Task.Run(() => Add(obj));
        }

        public T Add(T obj)
        {
            if (obj == null)
                throw new global::MyORM.Exceptions.ArgumentNullException($"The param {typeof(T)} {nameof(obj)} is null");

            _AddChildrenObjects(obj);

            StringBuilder sql = new StringBuilder();

            string tableName = typeof(T).GetCustomAttribute<DBTableAttribute>()?.Name ?? typeof(T).Name.ToLower();


            sql.Append($"INSERT INTO {_pGManager.PGConnectionBuilder.Schema}.{tableName}");

            List<PropertyInfo> propertyInfos = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(d => d.PropertyType == typeof(string) || d.PropertyType.IsValueType || d.PropertyType.IsAssignableTo(typeof(IEnumerable)))
                .Where(d => d.GetCustomAttribute<DBIgnoreAttribute>() == null)
                .Where(u => u.GetCustomAttribute<DBPrimaryKeyAttribute>() == null)
                .ToList();

            for (int i = 0; i < propertyInfos.Count; i++)
            {
                if (propertyInfos[i].PropertyType.IsAssignableTo(typeof(IEnumerable)) && propertyInfos[i].PropertyType != typeof(string))
                {
                    Type arrayType = propertyInfos[i].PropertyType.GetElementType() ?? propertyInfos[i].PropertyType.GetGenericArguments()[0];

                    bool isValueType = (!arrayType.IsClass) || arrayType == typeof(string);

                    if (!isValueType)
                        continue;
                }

                string colName = propertyInfos[i].GetCustomAttribute<DBColumnAttribute>()?.Name ?? propertyInfos[i].Name.ToLower();
                if (i == 0)
                {
                    sql.Append("( ");
                    sql.Append(colName);
                }
                else
                {
                    sql.Append($" , {colName} ");
                }

            }



            sql.Append(" ) VALUES ");


            for (int i = 0; i < propertyInfos.Count; i++)
            {
                string colName = propertyInfos[i].GetCustomAttribute<DBColumnAttribute>()?.Name ?? propertyInfos[i].Name.ToLower();

                bool isArray = propertyInfos[i].PropertyType.IsAssignableTo(typeof(IEnumerable)) && propertyInfos[i].PropertyType != typeof(string);


                PropertyInfo c = propertyInfos[i];

                if (i == 0)
                {
                    sql.Append("( ");

                }

                if (c.PropertyType == typeof(string))
                {
                    object? v = c.GetValue(obj);

                    sql.Append(v == null ? "null" : $"$${v?.ToString()?.Trim()}$$");
                }

                if (c.PropertyType == typeof(bool))
                {
                    object? v = c.GetValue(obj);

                    sql.Append(v == null ? "null" : $"{v?.ToString()?.Trim().ToLower()}");
                }

                if (c.PropertyType.IsEnum)
                {
                    object? v = (int)c.GetValue(obj);

                    sql.Append(v == null ? "null" : $"{v?.ToString()?.Trim()}");
                }

                if (c.PropertyType == typeof(int) || c.PropertyType == typeof(long))
                {
                    object? v = c.GetValue(obj);

                    sql.Append(v == null ? "null" : $"{v?.ToString()?.Trim()}");
                }

                if (c.PropertyType == typeof(decimal) || c.PropertyType == typeof(float) || c.PropertyType == typeof(double))
                {
                    object? v = c.GetValue(obj);

                    sql.Append(v == null ? "null" : $"{v?.ToString()?.Trim()}");
                }

                if (c.PropertyType == typeof(DateTime))
                {
                    try
                    {
                        DateTime? v = (DateTime?)c.GetValue(obj);

                        sql.Append(v == null ? "null" : $"'{v?.Year}-{v?.Month}-{v?.Day}'");
                    }
                    catch
                    {
                        sql.Append("null");
                    }
                }

                if (isArray)
                {

                    if (c.PropertyType.IsAssignableTo(typeof(IEnumerable)) && c.PropertyType != typeof(string))
                    {
                        Type arrayType = c.PropertyType.GetElementType() ?? c.PropertyType.GetGenericArguments()[0];

                        bool isValueType = (!arrayType.IsClass) || arrayType == typeof(string);

                        if (!isValueType)
                            continue;
                    }

                    IEnumerable? v = c.GetValue(obj) as IEnumerable;

                    if (v == null)
                    {
                        sql.Append("null");
                    }
                    else
                    {

                        StringBuilder values = new StringBuilder();

                        foreach (object u in v)
                        {
                            if (values.ToString().Length == 0)
                            {
                                values.Append(" array[ ");
                            }
                            else
                            {
                                values.Append(" , ");
                            }

                            if (u.GetType() == typeof(string))
                            {
                                values.Append($" $${u?.ToString() ?? ""}$$  ");
                            }

                            if (u.GetType() == typeof(bool))
                            {
                                object? vd = u;

                                sql.Append(vd == null ? "null" : $"{vd?.ToString()?.Trim().ToLower()}");
                            }

                            if (u.GetType().IsEnum)
                            {
                                object? vd = (int)u;

                                sql.Append(vd == null ? "null" : $"{vd?.ToString()?.Trim()}");
                            }

                            if (u.GetType() == typeof(DateTime))
                            {
                                DateTime? vd = (DateTime?)u;

                                values.Append($" '{vd?.Year}-{vd?.Month}-{vd?.Day}' ");
                            }

                            if (u.GetType() == typeof(int) || u.GetType() == typeof(long))
                            {
                                values.Append($"{u?.ToString()?.Trim()}");
                            }


                            if (u.GetType() == typeof(decimal) || u.GetType() == typeof(float) || u.GetType() == typeof(double))
                            {
                                values.Append($"{u?.ToString()?.Trim()}");
                            }



                        }

                        values.Append(" ] ");

                        sql.Append(values.ToString());
                    }


                }

                if (i == propertyInfos.Count - 1)
                {
                    sql.Append(" ) ");
                }
                else
                {
                    sql.Append(" , ");
                }


            }

            PropertyInfo? propKey = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(u => u.GetCustomAttribute<DBPrimaryKeyAttribute>() != null)
                .FirstOrDefault();

            if (propKey != null && propKey.SetMethod != null)
            {
                string colName = propKey.GetCustomAttribute<DBColumnAttribute>()?.Name ?? propKey.Name.ToLower();

                sql.Append($" RETURNING {colName} ");

                if (propKey.PropertyType == typeof(int))
                {

                    propKey.SetValue(obj, _pGManager.ExecuteScalar<int>(sql.ToString()));

                }
                else if (propKey.PropertyType == typeof(long))
                {
                    propKey.SetValue(obj, _pGManager.ExecuteScalar<long>(sql.ToString()));
                }
            }





            return obj;

        }

        private void _AddChildrenObjects(T obj)
        {

            List<PropertyInfo> propertyInfos = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance)
               .Where(d => d.PropertyType != typeof(string) && d.PropertyType.IsClass)
               .Where(d => d.GetCustomAttribute<DBIgnoreAttribute>() == null)
               .ToList();

            foreach (PropertyInfo subItem in propertyInfos)
            {

                if (subItem.GetValue(obj) == null)
                    continue;

                PropertyInfo propKey = subItem.PropertyType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(u => u.GetCustomAttribute<DBPrimaryKeyAttribute>() != null)
                .FirstOrDefault();

                if (propKey == null)
                    continue;


                PropertyInfo set = _context.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance)
                    .Where(d => d.PropertyType.GetInterfaces().Contains(typeof(IEntityCollection)))
                    .Where(d => d.PropertyType.GenericTypeArguments.Contains(subItem.PropertyType)).FirstOrDefault();


                if (set == null)
                    continue;

                string methodName = null;

                try
                {
                    methodName = Convert.ToInt64(propKey.GetValue(subItem.GetValue(obj)).ToString()) <= 0 ? "Add" : "Update";

                }
                catch(Exception ex)
                {
                    throw new InvalidConstraintException($"Can not get key value from {subItem.PropertyType}.{propKey.Name} property");
                }

                MethodInfo? methodToInvoke = set.PropertyType.GetMethod(methodName, new Type[] { subItem.PropertyType });

                if (methodToInvoke == null)
                    continue;


                methodToInvoke.Invoke(set.GetValue(_context), new object[] { subItem.GetValue(obj) });


                PropertyInfo foreignKey = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance)
                            .Where(u => u.Name == $"{subItem.Name}Id")
                            .FirstOrDefault();

                if (foreignKey == null || foreignKey.SetMethod == null)
                    continue;

                foreignKey.SetValue(obj, propKey.GetValue(subItem.GetValue(obj)));

            }
        }


        public async Task UpdateAsync(T obj)
        {
            await Task.Run(() => Update(obj));
        }

        public void Update(T obj)
        {
            if (obj == null)
                throw new global::MyORM.Exceptions.ArgumentNullException($"The param {typeof(T)} {nameof(obj)} is null");

            StringBuilder sql = new StringBuilder();

            string tableName = typeof(T).GetCustomAttribute<DBTableAttribute>()?.Name ?? typeof(T).Name.ToLower();

            sql.Append($"UPDATE {_pGManager.PGConnectionBuilder.Schema}.{tableName} SET ");

            List<PropertyInfo> fields = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(d => d.GetCustomAttribute<DBIgnoreAttribute>() == null)
                .Where(d => d.GetCustomAttribute<DBPrimaryKeyAttribute>() == null)
                .Where(d => d.PropertyType == typeof(string) || d.PropertyType.IsValueType || d.PropertyType.IsAssignableTo(typeof(IEnumerable)))
                .ToList();

            if (fields.Count == 0)
                throw new NoEntityMappedException($"No one key was mapped for {typeof(T).Name}");

            for (int i = 0; i < fields.Count; i++)
            {

                if (fields[i].PropertyType.IsAssignableTo(typeof(IEnumerable)) && fields[i].PropertyType != typeof(string))
                {
                    Type arrayType = fields[i].PropertyType.GetElementType() ?? fields[i].PropertyType.GetGenericArguments()[0];

                    bool isValueType = (!arrayType.IsClass) || arrayType == typeof(string);

                    if (!isValueType)
                        continue;
                }


                bool isArray = fields[i].PropertyType.IsAssignableTo(typeof(IEnumerable)) && fields[i].PropertyType != typeof(string);


                string colName = fields[i].GetCustomAttribute<DBColumnAttribute>()?.Name ?? fields[i].Name.ToLower();

                PropertyInfo c = fields[i];

                if (i > 0)
                {
                    sql.Append(" , ");

                }

                sql.Append($" {colName} = ");

                if (c.PropertyType == typeof(string))
                {
                    object? v = c.GetValue(obj);

                    sql.Append(v == null ? "null" : $"$${v?.ToString()?.Trim()}$$");
                }

                if (c.PropertyType == typeof(int) || c.PropertyType == typeof(long))
                {
                    object? v = c.GetValue(obj);

                    sql.Append(v == null ? "null" : $"{v?.ToString()?.Trim()}");
                }

                if (c.PropertyType == typeof(decimal) || c.PropertyType == typeof(float) || c.PropertyType == typeof(double))
                {
                    object? v = c.GetValue(obj);

                    sql.Append(v == null ? "null" : $"{v?.ToString()?.Trim()}");
                }


                if (c.PropertyType == typeof(bool))
                {
                    object? v = c.GetValue(obj);

                    sql.Append(v == null ? "null" : $"{v?.ToString()?.Trim().ToLower()}");
                }

                if (c.PropertyType.IsEnum)
                {
                    object? v = (int)c.GetValue(obj);

                    sql.Append(v == null ? "null" : $"{v?.ToString()?.Trim()}");
                }

                if (c.PropertyType == typeof(DateTime))
                {
                    try
                    {
                        DateTime? v = (DateTime?)c.GetValue(obj);

                        sql.Append(v == null ? "null" : $"'{v?.Year}-{v?.Month}-{v?.Day}'");
                    }
                    catch
                    {
                        sql.Append("null");
                    }
                }

                if (isArray)
                {

                    if (c.PropertyType.IsAssignableTo(typeof(IEnumerable)) && c.PropertyType != typeof(string))
                    {
                        Type arrayType = c.PropertyType.GetElementType() ?? c.PropertyType.GetGenericArguments()[0];

                        bool isValueType = (!arrayType.IsClass) || arrayType == typeof(string);

                        if (!isValueType)
                            continue;
                    }

                    IEnumerable? v = c.GetValue(obj) as IEnumerable;

                    if (v == null)
                    {
                        sql.Append("null");
                    }
                    else
                    {

                        StringBuilder values = new StringBuilder();

                        foreach (object u in v)
                        {
                            if (values.ToString().Length == 0)
                            {
                                values.Append(" array[ ");
                            }
                            else
                            {
                                values.Append(" , ");
                            }

                            if (u.GetType() == typeof(string))
                            {
                                values.Append($" $${u?.ToString() ?? ""}$$  ");
                            }

                            if (u.GetType() == typeof(DateTime))
                            {
                                DateTime? vd = (DateTime?)u;

                                values.Append($" '{vd?.Year}-{vd?.Month}-{vd?.Day}' ");
                            }

                            if (u.GetType() == typeof(int) || u.GetType() == typeof(long))
                            {
                                values.Append($"{u?.ToString()?.Trim()}");
                            }


                            if (u.GetType() == typeof(decimal) || u.GetType() == typeof(float) || u.GetType() == typeof(double))
                            {
                                values.Append($"{u?.ToString()?.Trim()}");
                            }

                        }

                        values.Append(" ] ");

                        sql.Append(values.ToString());
                    }


                }



            }

            sql.Append(" WHERE ");


            List<PropertyInfo> keys = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(d => d.GetCustomAttribute<DBIgnoreAttribute>() == null)
                .Where(d => d.GetCustomAttribute<DBPrimaryKeyAttribute>() != null && d.PropertyType.IsValueType)
                .ToList();

            if (keys.Count == 0)
                throw new NoEntityMappedException($"No one key was mapped for {typeof(T).Name}");

            for (int i = 0; i < keys.Count; i++)
            {
                string colName = keys[i].GetCustomAttribute<DBColumnAttribute>()?.Name ?? keys[i].Name.ToLower();

                PropertyInfo c = keys[i];

                if (i > 0)
                {
                    sql.Append(" AND ");

                }

                sql.Append($" {colName} = ");

                if (c.PropertyType == typeof(string))
                {
                    object? v = c.GetValue(obj);

                    sql.Append(v == null ? "null" : $"$${v?.ToString()?.Trim()}$$");
                }

                if (c.PropertyType == typeof(int) || c.PropertyType == typeof(long))
                {
                    object? v = c.GetValue(obj);

                    sql.Append(v == null ? "null" : $"{v?.ToString()?.Trim()}");
                }

                if (c.PropertyType == typeof(decimal) || c.PropertyType == typeof(float) || c.PropertyType == typeof(double))
                {
                    object? v = c.GetValue(obj);

                    sql.Append(v == null ? "null" : $"{v?.ToString()?.Trim()}");
                }

                if (c.PropertyType == typeof(DateTime))
                {
                    try
                    {
                        DateTime? v = (DateTime?)c.GetValue(obj);

                        sql.Append(v == null ? "null" : $"'{v?.Year}-{v?.Month}-{v?.Day}'");
                    }
                    catch
                    {
                        sql.Append("null");
                    }
                }



            }

            _pGManager.ExecuteNonQuery(sql.ToString());


            List<PropertyInfo> childrenObjects = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance)
               .Where(d => d.GetCustomAttribute<DBIgnoreAttribute>() == null)
               .Where(d => d.PropertyType.IsClass && _context.MappedTypes.Contains(d.PropertyType))
               .ToList();


            foreach(PropertyInfo subItem in childrenObjects)
            {
                if (subItem.GetValue(obj) == null)
                    continue;

                PropertyInfo propKey = subItem.PropertyType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                    .Where(d => d.GetCustomAttribute<DBIgnoreAttribute>() == null)
                    .Where(d => d.GetCustomAttribute<DBPrimaryKeyAttribute>() != null).FirstOrDefault();

                if (propKey == null)
                    continue;

                string methodName = null;

                try
                {
                    methodName = Convert.ToInt64(propKey.GetValue(subItem.GetValue(obj)).ToString()) <= 0 ? "Add" : "Update";

                }
                catch (Exception ex)
                {
                    throw new InvalidConstraintException($"Can not get key value from {subItem.PropertyType}.{propKey.Name} property");
                }                              


                IEntityCollection collection = _context.Collection(subItem.PropertyType);

                MethodInfo? set = collection.GetType().GetMethod(methodName, new Type[] { subItem.PropertyType });

                if (set != null)
                {
                    set.Invoke(collection, new object[] { subItem.GetValue(obj) });
                }

            }

            


        }



        public async Task DeleteAsync(T obj)
        {
            await Task.Run(() => Delete(obj));
        }

        public void Delete(T obj)
        {

            if (obj == null)
                throw new global::MyORM.Exceptions.ArgumentNullException($"The param {typeof(T)} {nameof(obj)} is null");

            StringBuilder sql = new StringBuilder();

            string tableName = typeof(T).GetCustomAttribute<DBTableAttribute>()?.Name ?? typeof(T).Name.ToLower();

            sql.Append($"DELETE FROM {_pGManager.PGConnectionBuilder.Schema}.{tableName} WHERE");

            List<PropertyInfo> keys = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(d => d.GetCustomAttribute<DBIgnoreAttribute>() == null)
                .Where(d => d.GetCustomAttribute<DBPrimaryKeyAttribute>() != null && d.PropertyType.IsValueType)
                .ToList();

            if (keys.Count == 0)
                throw new NoEntityMappedException($"No one key was mapped for {typeof(T).Name}");

            for (int i = 0; i < keys.Count; i++)
            {
                string colName = keys[i].GetCustomAttribute<DBColumnAttribute>()?.Name ?? keys[i].Name.ToLower();

                PropertyInfo c = keys[i];

                if (i > 0)
                {
                    sql.Append(" AND ");

                }

                sql.Append($" {colName} = ");

                if (c.PropertyType == typeof(string))
                {
                    object? v = c.GetValue(obj);

                    sql.Append(v == null ? "null" : $"$${v?.ToString()?.Trim()}$$");
                }

                if (c.PropertyType == typeof(int) || c.PropertyType == typeof(long))
                {
                    object? v = c.GetValue(obj);

                    sql.Append(v == null ? "null" : $"{v?.ToString()?.Trim()}");
                }

                if (c.PropertyType == typeof(decimal) || c.PropertyType == typeof(float) || c.PropertyType == typeof(double))
                {
                    object? v = c.GetValue(obj);

                    sql.Append(v == null ? "null" : $"{v?.ToString()?.Trim()}");
                }

                if (c.PropertyType == typeof(DateTime))
                {
                    try
                    {
                        DateTime? v = (DateTime?)c.GetValue(obj);

                        sql.Append(v == null ? "null" : $"'{v?.Year}-{v?.Month}-{v?.Day}'");
                    }
                    catch
                    {
                        sql.Append("null");
                    }
                }



            }

            _pGManager.ExecuteNonQuery(sql.ToString());
        }

        public IQueryableCollection<T> Join<TResult>(Expression<Func<T, TResult>> expression)
        {
            MemberExpression? memberExpression = expression.Body as MemberExpression;

            if (memberExpression == null)
            {
                throw new global::MyORM.Exceptions.InvalidExpressionException($"The lambda expression {expression.Body.ToString()} is not a valid member expression");
            }

            PropertyInfo? member = memberExpression.Member as PropertyInfo;

            if (memberExpression == null)
            {
                throw new global::MyORM.Exceptions.InvalidMemberForExpressionException($"Can´t read the PropertyInfo of the member of expression");
            }

            if (!(member.ReflectedType == typeof(T) || typeof(T).IsSubclassOf(member.ReflectedType)))
            {
                throw new global::MyORM.Exceptions.InvalidMemberForExpressionException($"The property {member.Name} is not a property of {typeof(T).Name}");

            }

            if (member.PropertyType.IsValueType || member.PropertyType == typeof(string))
            {
                return this;

            }

            PropertyInfo? foreignKey = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(d => d.GetCustomAttribute<DBIgnoreAttribute>() == null)
                .Where(d => d.PropertyType.IsValueType)
                .Where(d => d.Name == $"{member.Name}Id").FirstOrDefault();

            if (foreignKey == null)
            {
                throw new NoEntityMappedException($"No one foreign key was mapped for {member.PropertyType.Name}");
            }


            string tableName = member.PropertyType.GetCustomAttribute<DBTableAttribute>()?.Name ?? member.PropertyType.Name.ToLower();

            PropertyInfo? key = member.PropertyType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
               .Where(d => d.GetCustomAttribute<DBIgnoreAttribute>() == null)
               .Where(d => d.GetCustomAttribute<DBPrimaryKeyAttribute>() != null)
               .Where(d => d.PropertyType.IsValueType)
               .FirstOrDefault();

            string colName = key.GetCustomAttribute<DBColumnAttribute>()?.Name ?? key.Name.ToLower();


            string sql = $"SELECT * FROM {_pGManager.PGConnectionBuilder.Schema}.{tableName} WHERE {colName} in ";


            PGCollection<T> result = new PGCollection<T>(_pGManager, _context);

            result._sql = _sql;

            foreach ((PropertyInfo infoS, string sqlS) in _subTypes)
            {
                result._subTypes.Add(new(infoS, sqlS));
            }

            result._subTypes.Add(new(member, sql));


            return result;
        }
    }
}
