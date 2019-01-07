using System;
using System.Collections.Generic;

namespace MyNoSqlServer.Domains.Db
{
    
    
    public static class DbRowUtils
    {
        
        public static IEnumerable<byte> ToJsonArray(this IEnumerable<DbRow> dbRows)
        {
            yield return (byte) '[';

            var firstLine = true;
            foreach (var row in dbRows)
            {
                if (firstLine)
                    firstLine = false;
                else
                    yield return (byte) ',';

                foreach (var b in row.Data)
                    yield return b;
            }

            yield return (byte) ']';

        }
        
        public static IEnumerable<byte> InjectTimestamp(this byte[] byteArray, string timeStamp)
        {
            var waitingForOpenedTag = true;

            foreach (var b in byteArray)
            {
                yield return b;
                
                if (waitingForOpenedTag)
                {
                    if (b == (byte)'{')
                    {
                        var fieldToYield = $"\"Timestamp\":\"{timeStamp}\",";

                        foreach (var c in fieldToYield)
                            yield return (byte) c;
                        
                        waitingForOpenedTag = false;
                    }
                }
                
            }
        }

        public static IEnumerable<ReadOnlyMemory<byte>> SplitByDbRows(this byte[] byteArray)
        {
            var objectLevel = 0;
            var startIndex = -1;

            var insideString = false;
            var escapeMode = false;

            
            for (var i=0; i<byteArray.Length; i++)
            {
                if (escapeMode)
                {
                    escapeMode = false;
                    continue;
                }

                switch (byteArray[i])
                {

                    case (byte) '\\':
                        if (insideString)
                          escapeMode = true;
                        break;
                    
                    case (byte) '"':
                        insideString = !insideString;
                        break;

                    case (byte) '{':
                        if (!insideString)
                        {
                            objectLevel++;
                            if (objectLevel == 1)
                                startIndex = i;
                        }

                        break;

                    case (byte) '}':
                        if (!insideString)
                        {
                            objectLevel--;
                            if (objectLevel == 0)
                                yield return new ReadOnlyMemory<byte>(byteArray, startIndex, i - startIndex+1);
                        }

                        break;
                }

            }

        }
        
    }
}