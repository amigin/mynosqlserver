using System;
using System.Text;

namespace Common
{
    public static class Utils
    {

        public static int ParseIntOrDefault(this string line, int @default = 0)
        {
            try
            {
                return int.Parse(line);
            }
            catch (Exception)
            {
                return @default;
            }
        }


        public static string AddLastSymbolIfOneNotExists(this string line, char theChar)
        {
            if (line == null)
                return line;

            if (line == string.Empty)
                return string.Empty+theChar;


            if (line[line.Length - 1] == theChar)
                return line;

            return line + theChar;

        }


        public static string ToBase64(this string src)
        {
            var bytes = Encoding.UTF8.GetBytes(src);
            return Convert.ToBase64String(bytes);
        }
        
        public static string Base64ToString(this string src)
        {            
            var bytes = Convert.FromBase64String(src);;
            return Encoding.UTF8.GetString(bytes);
        }

        public static T[] ToSingleArray<T>(this T value)
        {
            return new[] {value};
        }

        
    }
    
}