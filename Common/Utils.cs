using System;

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
        
    }
    
}