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
        
        public static IEnumerable<byte> InjectTimeStamp(this byte[] data, string timeStamp, Dictionary<string, string> keyValue = null)
        {
            
            var copy = keyValue != null ? new Dictionary<string,string>(keyValue) : new Dictionary<string, string>();
            
            var valueToInject =$"\"{timeStamp}\"" ;

            yield return JsonByteArrayReader.OpenBracket;
            
            foreach (var (keySpan, valueSpan) in data.ParseFirstLevelOfJson())
            {
                
                if (keySpan.IsTimeStampField())
                    continue;

                foreach (var b in keySpan)
                    yield return b;
                
                yield return JsonByteArrayReader.DoubleColumn;
                
                foreach (var b in valueSpan)
                    yield return b;                
                
                yield return JsonByteArrayReader.Comma;


                if (keyValue != null && copy.Count > 0)
                {
                    var key = keySpan.AsString().RemoveDoubleQuotes();

                    if (keyValue.ContainsKey(key))
                        keyValue[key] = valueSpan.AsString().RemoveDoubleQuotes();

                    copy.Remove(key);
                }
                
            }
            
            foreach (var c in FieldToYield)
                yield return (byte) c;
                
            yield return JsonByteArrayReader.DoubleColumn;
            
            foreach (var c in valueToInject)
                yield return (byte) c;

            yield return JsonByteArrayReader.CloseBracket;
            
        }
        
    }
    
}