using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Linq.Expressions;
using MyORM.Interfaces;

namespace MyORMForPostgreSQL.Objects
{
    public static class PGCollectionLinq
    {
        public static IQueryableCollection<TSource> Where<TSource, TResult>(this IQueryableCollection<TSource> source, Expression<Func<TSource,TResult>> expression) where TSource : class
        {
            return source.Query(expression);
        }

        public static IEnumerable<TSource> ToList<TSource, TResult>(this IQueryableCollection<TSource> source, Expression<Func<TSource, TResult>> expression) where TSource : class
        {
            return source.Query(expression).Run();
        }

        public static IEnumerable<TSource> ToList<TSource>(this IQueryableCollection<TSource> source) where TSource : class
        {
            return source.Run();
        }

        public static async Task<IEnumerable<TSource>> ToListAsync<TSource, TResult>(this IQueryableCollection<TSource> source, Expression<Func<TSource, TResult>> expression) where TSource : class
        {
            return await source.Query(expression).RunAsync();
        }

        public static async Task<IEnumerable<TSource>> ToListAsync<TSource>(this IQueryableCollection<TSource> source) where TSource : class
        {
            return await source.RunAsync();
        }

        public static IEnumerable<TSource> Take<TSource>(this IQueryableCollection<TSource> source, int limit) where TSource : class
        {
            return  source.Limit(limit).Run();
        }

        public static async Task<IEnumerable<TSource>> TakeAsync<TSource>(this IQueryableCollection<TSource> source, int limit) where TSource : class
        {
            return await source.Limit(limit).RunAsync();
        }

        public static TSource First<TSource>(this IQueryableCollection<TSource> source) where TSource : class
        {
            return source.Limit(1).Run().FirstOrDefault();
        }

        public static async Task<TSource> FirstAsync<TSource>(this IQueryableCollection<TSource> source) where TSource : class
        {
            return (await source.Limit(1).RunAsync()).FirstOrDefault();
        }


    }
}
