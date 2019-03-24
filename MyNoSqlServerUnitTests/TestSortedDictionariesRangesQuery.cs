using System.Collections.Generic;
using System.Linq;
using Common;
using Xunit;

namespace MyNoSqlServerUnitTests
{
    public class TestSortedDictionariesRangesQuery
    {
        
        private static readonly SortedList<string, string> SmallAmount = new SortedList<string, string>
        {
            ["00"] = "00",
            ["02"] = "02",
            ["04"] = "04",
            ["06"] = "06",
            ["08"] = "08",
        };
        
        private static readonly SortedList<string, string> LargeAmount = new SortedList<string, string>
        {
            ["00"] = "00",
            ["02"] = "02",
            ["04"] = "04",
            ["06"] = "06",
            ["08"] = "08",
            ["10"] = "10",
            ["12"] = "12",
            ["14"] = "14",
            ["16"] = "16",
            ["18"] = "18",
            ["20"] = "20",
            ["22"] = "22",
            ["24"] = "24",
            ["26"] = "26",
            ["28"] = "28",
            ["30"] = "30",
            ["32"] = "32",
        };
        

        [Fact]
        public void TestBetweenWithMinimumAmountToSearch()
        {

            var result = SmallAmount.GetRange("02", "06", RangeInclude.Both).ToArray();
            
            Assert.Equal(3,result.Length);
            
            Assert.Equal("02", result[0]);
            Assert.Equal("04", result[1]);
            Assert.Equal("06", result[2]);
            
            result = SmallAmount.GetRange("01", "06", RangeInclude.Both).ToArray();
            
            Assert.Equal(3,result.Length);
            
            Assert.Equal("02", result[0]);
            Assert.Equal("04", result[1]);
            Assert.Equal("06", result[2]);
            
            
            result = SmallAmount.GetRange("02", "06", RangeInclude.Lower).ToArray();
            
            Assert.Equal(2,result.Length);
            
            Assert.Equal("02", result[0]);
            Assert.Equal("04", result[1]);
            
            result = SmallAmount.GetRange("02", "06", RangeInclude.Higher).ToArray();
            
            Assert.Equal(2,result.Length);
            
            Assert.Equal("04", result[0]);
            Assert.Equal("06", result[1]);
        }
        
        [Fact]
        public void TestBetweenWithLargeAmountToSearch()
        {

            var result = LargeAmount.GetRange("11", "15", RangeInclude.Both).ToArray();
            
            Assert.Equal(2,result.Length);
            
            Assert.Equal("12", result[0]);
            Assert.Equal("14", result[1]);
            
            
            
            result = LargeAmount.GetRange("02", "05", RangeInclude.Lower).ToArray();
            
            Assert.Equal(2,result.Length);
            
            Assert.Equal("02", result[0]);
            Assert.Equal("04", result[1]);
            
            
            result = LargeAmount.GetRange("02", "06", RangeInclude.Lower).ToArray();
            
            Assert.Equal(2,result.Length);
            
            Assert.Equal("02", result[0]);
            Assert.Equal("04", result[1]);
            
            result = LargeAmount.GetRange("01", "06", RangeInclude.Lower).ToArray();
            
            Assert.Equal(2,result.Length);
            
            Assert.Equal("02", result[0]);
            Assert.Equal("04", result[1]);
            
            
            result = LargeAmount.GetRange("10", "14", RangeInclude.Higher).ToArray();
            
            Assert.Equal(2,result.Length);
            
            Assert.Equal("12", result[0]);
            Assert.Equal("14", result[1]);

            result = LargeAmount.GetRange("11", "14", RangeInclude.Higher).ToArray();
            
            Assert.Equal(2,result.Length);
            
            Assert.Equal("12", result[0]);
            Assert.Equal("14", result[1]);
            
            result = LargeAmount.GetRange("11", "15", RangeInclude.Higher).ToArray();
            
            Assert.Equal(2,result.Length);
            
            Assert.Equal("12", result[0]);
            Assert.Equal("14", result[1]);
 
        }

        [Fact]
        public void GetLowerRangeWithSmallAmountOfItems()
        {
            var result = SmallAmount.GetLowerRange( "06", true).ToArray();
            
            Assert.Equal(4,result.Length);
            
             
            Assert.Equal("00", result[0]);
            Assert.Equal("02", result[1]);
            Assert.Equal("04", result[2]);
            Assert.Equal("06", result[3]);
            
            result = SmallAmount.GetLowerRange( "07", true).ToArray();
            
            Assert.Equal(4,result.Length);
            
             
            Assert.Equal("00", result[0]);
            Assert.Equal("02", result[1]);
            Assert.Equal("04", result[2]);
            Assert.Equal("06", result[3]);
            
            
            result = SmallAmount.GetLowerRange( "06", false).ToArray();
            
            
            Assert.Equal(3,result.Length);
             
            Assert.Equal("00", result[0]);
            Assert.Equal("02", result[1]);
            Assert.Equal("04", result[2]);
            
            result = SmallAmount.GetLowerRange( "05", false).ToArray();
            
            Assert.Equal(3,result.Length);
             
            Assert.Equal("00", result[0]);
            Assert.Equal("02", result[1]);
            Assert.Equal("04", result[2]);            
        }
        
        [Fact]
        public void GetLowerRangeWithLargeAmountOfItems()
        {
            var result = LargeAmount.GetLowerRange( "08", true).ToArray();
            
            Assert.Equal(5,result.Length);
            
            Assert.Equal("00", result[0]);             
            Assert.Equal("02", result[1]);
            Assert.Equal("04", result[2]);
            Assert.Equal("06", result[3]);
            Assert.Equal("08", result[4]);
            
            result = LargeAmount.GetLowerRange( "09", true).ToArray();
            
            Assert.Equal(5,result.Length);
            
            Assert.Equal("00", result[0]);             
            Assert.Equal("02", result[1]);
            Assert.Equal("04", result[2]);
            Assert.Equal("06", result[3]);
            Assert.Equal("08", result[4]);
            
            result = LargeAmount.GetLowerRange( "07", true).ToArray();
            
            Assert.Equal(4,result.Length);
            
            Assert.Equal("00", result[0]);             
            Assert.Equal("02", result[1]);
            Assert.Equal("04", result[2]);
            Assert.Equal("06", result[3]);
            
            result = LargeAmount.GetLowerRange( "08", false).ToArray();

            Assert.Equal(4,result.Length);
             
            Assert.Equal("00", result[0]);             
            Assert.Equal("02", result[1]);
            Assert.Equal("04", result[2]);
            Assert.Equal("06", result[3]);
            
            result = LargeAmount.GetLowerRange( "09", false).ToArray();

            Assert.Equal(5,result.Length);
             
            Assert.Equal("00", result[0]);             
            Assert.Equal("02", result[1]);
            Assert.Equal("04", result[2]);
            Assert.Equal("06", result[3]);
            Assert.Equal("08", result[4]);
            
            result = LargeAmount.GetLowerRange( "07", false).ToArray();

            Assert.Equal(4,result.Length);
             
            Assert.Equal("00", result[0]);             
            Assert.Equal("02", result[1]);
            Assert.Equal("04", result[2]);
            Assert.Equal("06", result[3]);
        }
        
        [Fact]
        public void GetHigherRangeWithSmallAmountOfItems()
        {
            var result = SmallAmount.GetGreaterRange( "02", true).ToArray();
            
            Assert.Equal(4,result.Length);
            
             
            Assert.Equal("02", result[0]);
            Assert.Equal("04", result[1]);
            Assert.Equal("06", result[2]);
            Assert.Equal("08", result[3]);
            
            result = SmallAmount.GetGreaterRange( "01", true).ToArray();
            
            Assert.Equal(4,result.Length);
            
             
            Assert.Equal("02", result[0]);
            Assert.Equal("04", result[1]);
            Assert.Equal("06", result[2]);
            Assert.Equal("08", result[3]);
            
            result = SmallAmount.GetGreaterRange( "03", true).ToArray();
            
            Assert.Equal(3,result.Length);
             
            Assert.Equal("04", result[0]);
            Assert.Equal("06", result[1]);
            Assert.Equal("08", result[2]);            
            
            result = SmallAmount.GetGreaterRange( "02", false).ToArray();
            
            Assert.Equal(3,result.Length);
             
            Assert.Equal("04", result[0]);
            Assert.Equal("06", result[1]);
            Assert.Equal("08", result[2]);
            
            result = SmallAmount.GetGreaterRange( "03", false).ToArray();
            
            Assert.Equal(3,result.Length);
             
            Assert.Equal("04", result[0]);
            Assert.Equal("06", result[1]);
            Assert.Equal("08", result[2]);
            
            result = SmallAmount.GetGreaterRange( "01", false).ToArray();
            
            Assert.Equal(4,result.Length);
             
            Assert.Equal("02", result[0]);
            Assert.Equal("04", result[1]);
            Assert.Equal("06", result[2]);
            Assert.Equal("08", result[3]);
            
            
        }

        [Fact] 
        public void GetHigherRangeWithLargeAmountOfItems()
        {
            var result = LargeAmount.GetGreaterRange( "26", true).ToArray();
            
            Assert.Equal(4,result.Length);
             
            Assert.Equal("26", result[0]);
            Assert.Equal("28", result[1]);
            Assert.Equal("30", result[2]);
            Assert.Equal("32", result[3]);
            
            result = LargeAmount.GetGreaterRange( "25", true).ToArray();
            
            Assert.Equal(4,result.Length);
             
            Assert.Equal("26", result[0]);
            Assert.Equal("28", result[1]);
            Assert.Equal("30", result[2]);
            Assert.Equal("32", result[3]);

            result = LargeAmount.GetGreaterRange( "27", true).ToArray();
            
            Assert.Equal(3,result.Length);
             
            Assert.Equal("28", result[0]);
            Assert.Equal("30", result[1]);
            Assert.Equal("32", result[2]);
            
            
            result = LargeAmount.GetGreaterRange( "26", false).ToArray();
            
            
            Assert.Equal(3,result.Length);
             
            Assert.Equal("28", result[0]);
            Assert.Equal("30", result[1]);
            Assert.Equal("32", result[2]);
            
            result = LargeAmount.GetGreaterRange( "25", false).ToArray();
            
            
            Assert.Equal(4,result.Length);
             
            Assert.Equal("26", result[0]);
            Assert.Equal("28", result[1]);
            Assert.Equal("30", result[2]);
            Assert.Equal("32", result[3]);
            
            result = LargeAmount.GetGreaterRange( "27", false).ToArray();
            
            
            Assert.Equal(3,result.Length);
             
            Assert.Equal("28", result[0]);
            Assert.Equal("30", result[1]);
            Assert.Equal("32", result[2]);
        }


    }
}