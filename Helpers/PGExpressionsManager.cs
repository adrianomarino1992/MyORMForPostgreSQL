using MyORM.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace MyORMForPostgreSQL.Helpers
{
    public static class PGExpressionsManager
    {
        public static IEnumerable<(ExpressionType?, Expression?)> SplitInBinaryExpressions(ExpressionType? type, Expression? expression)
        {
            List<(ExpressionType?, Expression?)> list = new List<(ExpressionType?, Expression?)>();


            BinaryExpression? action = expression as BinaryExpression;

            if (action == null)
            {
                MethodCallExpression? methodAction = expression as MethodCallExpression;

                if (methodAction != null)
                {
                    list.Add((type, methodAction));
                }

                return list;
            }


            if (new ExpressionType[]
            {
                ExpressionType.Or,
                ExpressionType.OrElse,
                ExpressionType.And,
                ExpressionType.AndAlso

            }.Contains(action.NodeType))
            {
                list.AddRange(SplitInBinaryExpressions(action.NodeType, action.Left));
                list.AddRange(SplitInBinaryExpressions(action.NodeType, action.Right));
            }
            else
            {
                list.Add((type, action));

            }

            return list;
        }

        public static string ManageBinaryExpressions<TEntity>(IEnumerable<(ExpressionType?, Expression?)> list) where TEntity : class
        {
            StringBuilder result = new StringBuilder();

            PropertyInfo[] propertyInfos = typeof(TEntity).GetProperties(BindingFlags.Public | BindingFlags.Instance);

            if (list.Count() == 0)
                return "";

            foreach ((ExpressionType? type, Expression? expression) in list.ToList())
            {

                Expression? ex = new Visitor().Visit(expression);

                if (list.Count() > 1 && result.ToString() != String.Empty)
                {
                    if (new ExpressionType?[]
                    {
                            ExpressionType.Or,
                            ExpressionType.OrElse

                    }
                    .Contains(type))
                    {
                        result.Append(" OR ");
                    }

                    if (new ExpressionType?[]
                    {
                            ExpressionType.And,
                            ExpressionType.AndAlso

                    }
                    .Contains(type))
                    {
                        result.Append(" AND ");
                    }
                }

               
                foreach (PropertyInfo info in propertyInfos)
                {

                    BinaryExpression? binaryEx = ex as BinaryExpression;

                    if (binaryEx != null)
                    {

                        Expression leftExpression = binaryEx.Left as MemberExpression;

                        if (leftExpression == null && binaryEx.Left is UnaryExpression unaryExpression)
                        {
                            leftExpression = unaryExpression.Operand as MemberExpression;
                        }

                        leftExpression = leftExpression ?? binaryEx.Left;

                        DBColumnAttribute? colName = info.GetCustomAttribute<DBColumnAttribute>();

                        string? leftSize = leftExpression.ToString().Split(new String[] { "." }, StringSplitOptions.None)[1].ToLower().Trim();

                        if (info.PropertyType == typeof(string) && info.Name.ToLower() == leftSize)
                        {
                            Expression rightExpression = binaryEx?.Right as Expression;

                            string? rightSize = "";

                            if (rightExpression != null)
                            {
                                rightSize = GetBinaryValue(rightExpression);
                            }
                            else
                            {
                                rightSize = binaryEx?.Right.ToString().Substring(1, binaryEx?.Right.ToString().Length - 2 ?? 0).Trim();

                            }

                            result.Append($" {(String.IsNullOrEmpty(colName?.Name) ? leftSize : colName.Name)} {GetBinaryOperator(binaryEx)} $${rightSize}$$ ");
                        }
                        else if ((!info.PropertyType.IsClass) && info.Name.ToLower() == leftSize)
                        {

                            Expression rightExpression = binaryEx?.Right as Expression;

                            string? rightSize = "";

                            if (rightExpression != null)
                            {
                                rightSize = GetBinaryValue(rightExpression);
                            }
                            else
                            {
                                rightSize = binaryEx?.Right.ToString().ToLower().Trim();

                            }
                            

                            result.Append($" {(String.IsNullOrEmpty(colName?.Name) ? leftSize : colName.Name)} {GetBinaryOperator(binaryEx)} {rightSize} ");
                        }
                    }

                }


                MethodCallExpression? methodCallEx = ex as MethodCallExpression;

                if (methodCallEx != null)
                {
                    PropertyInfo? info = ((methodCallEx.Object as MemberExpression)?.Member as PropertyInfo);

                    if (info != null && (info.PropertyType == typeof(string) || info.PropertyType.IsValueType))
                    {
                        

                        string? colName = info?.GetCustomAttribute<DBColumnAttribute>()?.Name ?? info?.Name.ToLower();

                        string? value = methodCallEx.Arguments[0].ToString();

                        if (value.StartsWith("\""))
                            value = value.Substring(1);

                        if (value.EndsWith("\""))
                            value = value.TrimEnd('\"');

                        if (methodCallEx.Method.Name == "Contains")
                        {
                            result.Append($" {colName} ilike $$%{value}%$$ ");
                        }

                        if (methodCallEx.Method.Name == "StartsWith")
                        {
                            result.Append($" {colName} ilike $${value}%$$ ");
                        }

                        if (methodCallEx.Method.Name == "EndsWith")
                        {
                            result.Append($" {colName} ilike $$%{value}$$ ");
                        }

                        if (methodCallEx.Method.Name == "Equals")
                        {
                            result.Append($" {colName} = $${value}$$ ");
                        }
                    }


                }

                
            }

            return result.ToString();
        }

        public static string GetBinaryValue(Expression? expression)
        {

            if(expression?.NodeType == ExpressionType.Constant)
            {
                var value =  (expression as ConstantExpression)?.Value;

                return value?.ToString() ?? "";

            }else if (expression?.NodeType == ExpressionType.Call)
            {
                MethodCallExpression? call = expression as MethodCallExpression;

                if(call != null)
                {
                    return GetBinaryValue(call?.Object);
                }

                return "";
            }
            else if (expression?.NodeType == ExpressionType.Convert)
            {
                UnaryExpression? call = expression as UnaryExpression;

                if (call != null)
                {
                    /*
                     * note : the Operand is a ConstantExpression, so, if we need a return with correct type
                     * we can cast Operand as ConstantExpression and return Value property
                     */
                    if (call.Operand.Type.IsEnum)
                    {
                        return ((int)(call.Operand as ConstantExpression).Value).ToString();
                    }
                    else
                    {

                        return call.Operand.ToString();
                    }
                }

                return "";
            }

            return "";
        }

        public static string GetBinaryOperator(BinaryExpression? expression)
        {

            if (expression?.NodeType == ExpressionType.Equal)
                return "=";

            if (expression?.NodeType == ExpressionType.NotEqual)
                return "!=";

            if (expression?.NodeType == ExpressionType.GreaterThan)
                return ">";

            if (expression?.NodeType == ExpressionType.GreaterThanOrEqual)
                return ">=";

            if (expression?.NodeType == ExpressionType.LessThan)
                return "<";

            if (expression?.NodeType == ExpressionType.LessThanOrEqual)
                return "<=";



            return "=";
        }
    }

    class Visitor : ExpressionVisitor
    {
        private Type _type = null;
        protected override Expression VisitMember(MemberExpression memberExpression)
        {            
            var expression = Visit(memberExpression.Expression);
                        
            if (expression is ConstantExpression)
            {
                object container = ((ConstantExpression)expression).Value;
                var member = memberExpression.Member;
                if (member is FieldInfo)
                {
                    object value = ((FieldInfo)member).GetValue(container);                    
                    return Expression.Constant(value);
                }
                if (member is PropertyInfo)
                {
                    object value = ((PropertyInfo)member).GetValue(container, null);
                    return Expression.Constant(value);
                }
            }
            return base.VisitMember(memberExpression);
        }

        protected override Expression VisitBinary(BinaryExpression node)
        {

            Expression left = Visit(node.Left);
            Expression righh = Visit(node.Right);
            
            try
            {
                
                return base.VisitBinary(node);
            }
            catch
            {
                BinaryExpression expression = Expression.Equal(left, righh);
                return base.VisitBinary(expression);
            }
            
        }

        protected override Expression VisitUnary(UnaryExpression node)
        {
            
            try
            {
                if (node.Type.Name.ToLower().Contains("nullable"))
                {
                    MemberExpression ex = node.Operand as MemberExpression;

                    if(ex == null || ex.Member.GetType().BaseType.Name == "RuntimeFieldInfo")
                    {
                        throw new Exception("Try get value");
                    }

                    if(ex.Member is PropertyInfo info)
                    {
                        _type = info.PropertyType;
                    }

                    return node.Operand;
                }
                else
                {

                    return base.VisitUnary(node);
                }
            }
            catch
            {
                object parameter = ((node.Operand as MemberExpression).Expression as ConstantExpression).Value;
                
                if(parameter == null)
                    return Expression.Constant(null);
                else
                {
                    try
                    {                       
                        object valueOfParameter = parameter.GetType().GetFields()[0].GetValue(parameter);

                        object parsed = System.Convert.ChangeType(valueOfParameter, _type);

                        return Expression.Constant(parsed);
                    }
                    catch
                    {
                        return Expression.Constant(null);
                    }
                }
            }
            
        }
    }
}
