using System.Text;
using MyNoSqlServer.Domains;

namespace MyNoSqlServerUnitTests
{
    public static class TestDecorators
    {

        public static byte[] AsByteArray(this IMyNoSqlDbEntity myNoSqlDbEntity)
        {
            return Encoding.UTF8.GetBytes(Newtonsoft.Json.JsonConvert.SerializeObject(myNoSqlDbEntity));
        }


        public static T DeserializeDbEntity<T>(this byte[] bytes)
        {
            var str = Encoding.UTF8.GetString(bytes);
            return Newtonsoft.Json.JsonConvert.DeserializeObject<T>(str);
        }
        
    }
}