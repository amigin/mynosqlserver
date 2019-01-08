using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Newtonsoft.Json;

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


        private const string FieldToYield = "\"Timestamp\"";
        private static bool IsTimeStampField(this ArraySpan span)
        {

            if (span.Length != FieldToYield.Length)
                return false;

            var i = 0;

            foreach (var b in span)
            {
                if (b != FieldToYield[i])
                    return false;
                i++;
            }

            return true;

        }
        
        public static IEnumerable<byte> InjectTimestamp(this byte[] byteArray, string timeStamp)
        {
            
            var valueToInject =$"\"{timeStamp}\"" ;

            yield return JsonByteArrayReader.OpenBracket;
            
            foreach (var (keySpan, valueSpan) in byteArray.ParseFirstLevelOfJson())
            {
                
                if (keySpan.IsTimeStampField())
                    continue;

                foreach (var b in keySpan)
                    yield return b;
                
                yield return JsonByteArrayReader.DoubleColumn;
                
                foreach (var b in valueSpan)
                    yield return b;                
                
                yield return JsonByteArrayReader.Comma;
                
            }
            
            foreach (var c in FieldToYield)
                yield return (byte) c;
                
            yield return JsonByteArrayReader.DoubleColumn;
            
            foreach (var c in valueToInject)
                yield return (byte) c;

            yield return JsonByteArrayReader.CloseBracket;
            
        }

        public static IEnumerable<ArraySpan> SplitByDbRows(this byte[] byteArray)
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
                                yield return new ArraySpan(byteArray)
                                {
                                    StartIndex = startIndex, 
                                    EndIndex = i
                                };  
                        }

                        break;
                }

            }

        }
        
    }
}