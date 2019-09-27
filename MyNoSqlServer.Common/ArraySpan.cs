using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MyNoSqlServer.Common
{
    public struct ArraySpan<T> : IEnumerable<T>
    {
        public ArraySpan(T[] array, int startIndex, int endIndex)
        {
            _array = array;
            _startIndex = startIndex;
            _endIndex = endIndex;
        }

        private readonly int _startIndex;
        private readonly int _endIndex;

        public int Length => _endIndex - _startIndex;

        private readonly T[] _array;

        public T[] AsArray()
        {
            var result = new T[Length];            
            Array.Copy(_array, _startIndex, result, 0, Length);
            return result;
        }
        
        public void CopyToArray(int srcOffset,  T[] destArray, int destOffset, int destLength)
        {
            Array.Copy(_array, srcOffset+_startIndex, destArray, destOffset, destLength);
        }

        public IEnumerator<T> GetEnumerator()
        {
            for (var i = _startIndex; i < _endIndex; i++)
                yield return _array[i];
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public override string ToString()
        {
            return this.AsString();
        }


        public static ArraySpan<T> CreateWithLength(T[] buffer, int startIndex, int length)
        {
            return new ArraySpan<T>(buffer, startIndex, startIndex+length);
        }

    }
    
    public static class ArraySpanHelpers
    {
        public static string AsString<T>(this ArraySpan<T> arraySpan)
        {
            
            if (typeof(T) == typeof(byte))
              return Encoding.UTF8.GetString(arraySpan.AsArray().Cast<byte>().ToArray());
            
            if (typeof(T) == typeof(char))
                return  new string(arraySpan.AsArray().Cast<char>().ToArray());


            return "Length: " + arraySpan.Length;

        }


        public static ArraySpan<T> ToByteArraySpan<T>(this T[] theArray, int startPosition, int length)
        {
            return ArraySpan<T>.CreateWithLength(theArray, startPosition, length);
        }

        public static ArraySpan<T> ToByteArraySpan<T>(this T[] theArray)
        {
            return ArraySpan<T>.CreateWithLength(theArray, 0, theArray.Length);
        }


        public static string RemoveDoubleQuotes(this string str)
        {
            if (str == null)
                return null;

            if (str.Length < 2)
                return str;


            if (str[0] == '"' && str[^1] == '"')
                return str.Length == 2 ? string.Empty : str.Substring(1, str.Length - 2);

            return str;
        }
    }
}