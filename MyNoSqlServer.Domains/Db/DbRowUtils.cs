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


        const string fieldToYield = "\"timestamp\"";
        private static bool IsTimeStampField(this byte[] byteArray, KeyValuePair<int, int> position)
        {
           

            if (position.Value - position.Key != fieldToYield.Length)
                return false;

            var j = 0;
            for (var i = position.Key; i < position.Value; i++)
            {
                if (byteArray[i] != fieldToYield[j])
                    return false;
                j++;
            }

            return true;

        }
        
        public static IEnumerable<byte> InjectTimestamp(this byte[] byteArray, string timeStamp)
        {
            
            var valueToInject =$"\"{timeStamp}\"" ;


            yield return JsonByteArrayReader.OpenBracket;



            
            foreach (var (key, value) in byteArray.ParseFirstLevelOfJson())
            {
                
                if (byteArray.IsTimeStampField(key))
                    continue;
                
                for (var i=key.Key; i<key.Value; i++)  
                    yield return byteArray[i];
                
                yield return JsonByteArrayReader.DoubleColumn;
                
                for (var i=value.Key; i<value.Value; i++)  
                    yield return byteArray[i];
                
                yield return JsonByteArrayReader.Comma;
                
            }
            
            foreach (var c in fieldToYield)
                yield return (byte) c;
                
            yield return JsonByteArrayReader.DoubleColumn;
            
            foreach (var c in valueToInject)
                yield return (byte) c;


            yield return JsonByteArrayReader.CloseBraked;
           
            
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