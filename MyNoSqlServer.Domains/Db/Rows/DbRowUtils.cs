using System.Collections.Generic;
using System.Text;
using Common;

namespace MyNoSqlServer.Domains.Db.Rows
{
    
    public static class DbRowUtils
    {
        public static ChunkedStream ToJsonArray(this IEnumerable<DbRow> dbRows)
        {
            var result = new ChunkedStream();
            result.Write(OpenArray);

            var firstLine = true;
            foreach (var row in dbRows)
            {
                if (firstLine)
                    firstLine = false;
                else
                    result.Write(Comma);

                result.Write(row.Data.ToByteArraySpan());
            }

            result.Write(CloseArray);

            return result;

        }

        private const string FieldToYield = "\"Timestamp\"";
        private static bool IsTimeStampField(this ArraySpan<byte> span)
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
        
        private static readonly ArraySpan<byte> OpenArray = new[] {JsonByteArrayReader.OpenArray}.ToByteArraySpan();
        private static readonly ArraySpan<byte> CloseArray = new[] {JsonByteArrayReader.CloseArray}.ToByteArraySpan();
        
        private static readonly ArraySpan<byte> OpenBracket = new[] {JsonByteArrayReader.OpenBracket}.ToByteArraySpan();
        private static readonly ArraySpan<byte> CloseBracket = new[] {JsonByteArrayReader.CloseBracket}.ToByteArraySpan();
        
        private static readonly ArraySpan<byte> DoubleColumn = new[] {JsonByteArrayReader.DoubleColumn}.ToByteArraySpan();
        private static readonly ArraySpan<byte> Comma = new[] {JsonByteArrayReader.Comma}.ToByteArraySpan();

        private static readonly ArraySpan<byte> TimestampFieldAndDoubleColumn = Encoding.UTF8.GetBytes(FieldToYield+":").ToByteArraySpan();
        public static ChunkedStream InjectTimeStamp(this byte[] data, string timeStamp, Dictionary<string, string> keyValue = null)
        {
            
            var copy = keyValue != null ? new Dictionary<string,string>(keyValue) : new Dictionary<string, string>();
            
            var valueToInject =$"\"{timeStamp}\"" ;
            
            var result = new ChunkedStream();

            
            result.Write(OpenBracket);

            
            foreach (var (keySpan, valueSpan) in data.ParseFirstLevelOfJson())
            {
                
                if (keySpan.IsTimeStampField())
                    continue;

                result.Write(keySpan);
                
                result.Write(DoubleColumn);
                
                result.Write(valueSpan);
                
                result.Write(Comma);


                if (keyValue != null && copy.Count > 0)
                {
                    var key = keySpan.AsString().RemoveDoubleQuotes();

                    if (keyValue.ContainsKey(key))
                        keyValue[key] = valueSpan.AsString().RemoveDoubleQuotes();

                    copy.Remove(key);
                }
                
            }
            
            
            result.Write(TimestampFieldAndDoubleColumn);
            
            result.Write(Encoding.UTF8.GetBytes(valueToInject).ToByteArraySpan());
            
            result.Write(CloseBracket);

            return result;

        }
        
    }
    
}