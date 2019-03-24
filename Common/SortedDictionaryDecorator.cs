using System;
using System.Collections.Generic;
using System.Linq;

namespace Common
{
    public enum RangeInclude
    {
        Both, Neither, Lower, Higher
    }
    
    public static class SortedListSearchDecorator
    {
        private const int MinAmountToScan = 10;

        private static Func<string, string, string, bool> GetRangePredicateFunction(this RangeInclude src)
        {
            switch (src)
            {
                case RangeInclude.Both:
                    return Utils.RangeBetweenIncludingBoth;
                
                case RangeInclude.Neither:
                    return Utils.RangeBetweenExcludingBoth;
                
                case RangeInclude.Lower:
                    return Utils.RangeBetweenIncludingLower;

                case RangeInclude.Higher:
                    return Utils.RangeBetweenIncludingHigher;

            }
            
            throw new Exception("Unknown Range Include: "+src);
        }

        public static Func<string, string, bool> GetGreaterComparator(this bool includeLower)
        {

            if (includeLower)
                return Utils.GreaterOrEqualThen;
            
            return Utils.GreaterThen;
        }

        public static Func<string, string, bool> GetLowerComparator(this bool includeHigher)
        {

            if (includeHigher)
                return Utils.LowerOrEqualThen;
            
            return Utils.LowerThen;
        }
        public static int FindNearest<TValue>(this SortedList<string, TValue> src, string value)
        {
            var min = 0;
            var max = src.Count-1;
            var position = max / 2;

            while (true)
            {

                var compareResult = string.CompareOrdinal(src.Keys[position], value);

                if (compareResult == 0)
                    return position;

                if (compareResult < 0)
                    min = position;
                else
                    max = position;
                
                position = min + (max-min) / 2;

                if (max - min <= 1)
                {

                    var minCompareResult = string.CompareOrdinal(src.Keys[min], value);
                    if (minCompareResult == 0)
                        return min;

                    var maxCompareResult = string.CompareOrdinal(src.Keys[max], value);
                    if (maxCompareResult == 0)
                        return max;

                    return minCompareResult > 0 
                        ? min : max;
                }

            }
        }



        
        public static IEnumerable<TValue> GetRange<TValue>(this SortedList<string, TValue> src, string keyFrom, string keyTo, RangeInclude rangeInclude)
        {


            var rangeIncludeFunc = rangeInclude.GetRangePredicateFunction();
            
            if (string.CompareOrdinal(keyFrom, keyTo) > 0)
                return Array.Empty<TValue>();
            
            if (src.Count < MinAmountToScan)
                return src.Where(itm => rangeIncludeFunc(itm.Key, keyFrom,keyTo))
                    .Select(itm => itm.Value);


            var fromIndex = src.FindNearest(keyFrom);

            var toIndex = src.FindNearest(keyTo);

            var result = new List<TValue>();

            for (var i = fromIndex; i <= toIndex; i++)
            {
                if ( rangeIncludeFunc(src.Keys[i], keyFrom, keyTo))
                    result.Add(src.Values[i]); 
            }

            return result;
        }
        
        public static IEnumerable<TValue> GetGreaterRange<TValue>(this SortedList<string, TValue> src, string keyFrom, bool includeLower)
        {

            var comparator = includeLower.GetGreaterComparator();

            if (src.Count < MinAmountToScan)
                return src.Where(itm =>  comparator(itm.Key, keyFrom))
                    .Select(itm => itm.Value);

            var fromIndex = src.FindNearest(keyFrom);
            fromIndex--;

            if (fromIndex < 0)
                fromIndex = 0;


            var result = new List<TValue>();

            for (var i = fromIndex; i < src.Count; i++)
            {
                if (comparator(src.Keys[i], keyFrom))
                    result.Add(src.Values[i]);
            }

            return result;
        }
        
        public static IEnumerable<TValue> GetLowerRange<TValue>(this SortedList<string, TValue> src, string keyTo, bool includeHigher)
        {

            var comparator = includeHigher.GetLowerComparator();
            
            if (src.Count < MinAmountToScan)
                return src.Where(itm =>  comparator(itm.Key, keyTo))
                    .Select(itm => itm.Value);

            var toIndex = src.FindNearest(keyTo);

            if (toIndex < src.Count - 1)
                toIndex++;


            var result = new List<TValue>();

            for (var i = 0; i < toIndex; i++)
            {
                if (comparator(src.Keys[i], keyTo))
                    result.Add(src.Values[i]);
            }

            return result;
        }
        
    }
}