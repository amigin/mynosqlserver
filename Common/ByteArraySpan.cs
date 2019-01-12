using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace Common
{
    public struct ByteArraySpan : IEnumerable<byte>
    {
        public ByteArraySpan(byte[] array, int startIndex, int endIndex)
        {
            _array = array;
            StartIndex = startIndex;
            EndIndex = endIndex;
        }
        
        public int StartIndex { get; }
        public int EndIndex { get; }

        public int Length => EndIndex - StartIndex;

        private readonly byte[] _array;

        public byte[] AsArray()
        {
            var result = new byte[Length];            
            Array.Copy(_array, StartIndex, result, 0, Length);
            return result;
        }

        
        
        public void CopyToArray(int srcOffset,  byte[] destArray, int destOffset, int destLength)
        {
            Array.Copy(_array, srcOffset+StartIndex, destArray, destOffset, destLength);
        }

        public IEnumerator<byte> GetEnumerator()
        {
            for (var i = StartIndex; i < EndIndex; i++)
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


        public static ByteArraySpan CreateWithLength(byte[] buffer, int startIndex, int length)
        {
            return new ByteArraySpan(buffer, startIndex, startIndex+length);
        }

    }
    
    public static class ArraySpanHelpers
    {
        public static string AsString(this ByteArraySpan arraySpan)
        {
            return Encoding.UTF8.GetString(arraySpan.AsArray());
        }


        public static ByteArraySpan ToByteArraySpan(this byte[] theArray, int startPosition, int length)
        {
            return ByteArraySpan.CreateWithLength(theArray, startPosition, length);
        }

        public static ByteArraySpan ToByteArraySpan(this byte[] theArray)
        {
            return ByteArraySpan.CreateWithLength(theArray, 0, theArray.Length);
        }

        public static string RemoveDoubleQuotes(this string str)
        {
            if (str == null)
                return str;

            if (str.Length < 2)
                return str;


            if (str[0] == '"' && str[str.Length - 1] == '"')
                return str.Length == 2 ? string.Empty : str.Substring(1, str.Length - 2);

            return str;
        }
    }
}