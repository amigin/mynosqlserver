using System.Collections.Generic;
using System.Linq;

namespace MyNoSqlServer.Domains.Query
{
    public static class SortedListQueryFilter
    {

        public static IEnumerable<T> FilterByQueryConditions<T>(this SortedList<string, T> src, IReadOnlyList<QueryCondition> queryConditions)
        {
            var fromConditions = queryConditions
                .Where(itm => itm.Operation == QueryOperation.Ge || itm.Operation == QueryOperation.Gt)
                .ToList();
            
            
            var toConditions = queryConditions
                .Where(itm => itm.Operation == QueryOperation.Le || itm.Operation == QueryOperation.Lt)
                .ToList();

            
            //ToDo - Develop and Unit-test it
            var result = new List<T>();

            return result;


        }
        
    }
}