using System;
using System.Collections.Generic;

namespace MyNoSqlServer.Domains.Query
{

    public enum QueryOperation
    {
        Eq, Gt, Lt, Ge, Le, Ne
    }

    public class QueryCondition
    {
        public string FieldName { get; set; }
        public QueryOperation Operation { get; set; }
        public string Value { get; set; }
    }

    public static class QueryBuilder
    {


        private static QueryOperation ParseQueryOperation(this string op)
        {
            op = op.ToLower();

            if (op == "eq")
                return QueryOperation.Eq;
            
            if (op == "gt")
                return QueryOperation.Gt;
            
            if (op == "lt")
                return QueryOperation.Lt;
            
            if (op == "ge")
                return QueryOperation.Ge;

            if (op == "le")
                return QueryOperation.Le;
            
            if (op == "ne")
                return QueryOperation.Ne;
            
            throw new Exception("Invalid query Operation");
            
        }


        private static int FindNext(this string query, int startFrom, Func<char, bool> condition)
        {
            for (var i = startFrom; i < query.Length; i++)
            {
                if (condition(query[i]))
                    return i;
            }

            return -1;
        }


        private const char StringChar = '\'';

        private static string ReadValue(this string query, int position)
        {
             position = query.FindNext(position, c => c > ' ');
            if (position == -1)
                throw new Exception("Invalid query: "+query);


            var isString = query[position] == StringChar; 

                
            var endPosition = query.FindNext(position+1, c =>
            {
                if (isString)
                    return c == StringChar;

                return c <= ' ';
            });
            
            if (endPosition == -1)
                endPosition = query.Length;

            if (isString)
                endPosition++;

            var result = query.Substring(position, endPosition-position);

            return result;
        }

        
        

        public static IEnumerable<QueryCondition> ParseQueryConditions(this string query)
        {
            var position = 0;

            while (position >= 0)
            {
               
                position = query.FindNext(position, c => c > ' ');
                
                if (position == -1)
                    break;

                var fieldName = query.ReadValue(position);
                position += fieldName.Length;
                
                position = query.FindNext(position, c => c > ' ');

                var op = query.ReadValue(position);
                position += op.Length;
                position = query.FindNext(position, c => c > ' ');
                
                var value =  query.ReadValue(position);
                position += value.Length;
                
                yield return new QueryCondition
                {
                    FieldName = fieldName,
                    Value = value,
                    Operation = op.ParseQueryOperation()
                };
                
                if (position >= query.Length)
                    break;
                
                position = query.FindNext(position, c => c > ' ');
                
                var separator =  query.ReadValue(position).ToLower();
                if (separator != "and")
                    throw new Exception("Only and logical operation is supported for a while");
                
                position += separator.Length;
            }

        }
        
        

    }
}