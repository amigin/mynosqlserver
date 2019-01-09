using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace MyNoSqlServer.Domains
{
    public class ArraySpan : IEnumerable<byte>
    {
        public ArraySpan(byte[] array)
        {
            _array = array;
        }
        
        public int StartIndex { get; internal set; }
        public int EndIndex { get; internal set; }

        public int Length => EndIndex - StartIndex;

        private readonly byte[] _array;

        public byte[] AsArray()
        {
            var result = new byte[Length];            
            Array.Copy(_array, StartIndex, result, 0, Length);
            return result;
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

    }
    
    public static class ArraySpanHelpers
    {
        public static string AsString(this ArraySpan arraySpan)
        {
            return Encoding.UTF8.GetString(arraySpan.AsArray());
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