using System;
using System.Collections.Generic;

namespace MyNoSqlServer.Common
{
    public static class ListUtils
    {

        public static void AddSlice<T>(this List<T> list, ReadOnlyMemory<T> slice, int startIndex = 0)
        {
            foreach (var b in slice.Slice(startIndex).Span)
                list.Add(b);    
        }
        
        public static void AddSlice<T>(this List<T> list, ReadOnlyMemory<T> slice, int startIndex, int len)
        {
            foreach (var b in slice.Slice(startIndex, len).Span)
                list.Add(b);    
        }

        
    }
}