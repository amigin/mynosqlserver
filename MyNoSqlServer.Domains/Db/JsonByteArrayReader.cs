using System;
using System.Collections.Generic;

namespace MyNoSqlServer.Domains.Db
{


    public enum ExpectedToken
    {
        OpenBracket, OpenKey, CloseKey, DoubleColumn, OpenValue, CloseStringValue, CloseNumberOrBoolValue, CloseObject, CloseArray, Comma, EndOfFile
    }
    
    public static class JsonByteArrayReader
    {
        
        public const byte OpenBracket = (byte) '{';
        public const byte CloseBraked = (byte) '}';
        public const byte DoubleQuote = (byte) '"';
        public const byte DoubleColumn = (byte) ':';
        public const byte OpenArray = (byte) '[';
        public const byte CloseArray = (byte) ']';            
        public const byte Comma = (byte) ',';
        public const byte EscSymbol = (byte) '\\';
        
        private static readonly Dictionary<char, char> StartOfDigit = new Dictionary<char, char>
        {
            ['0']='0',
            ['1']='1',
            ['2']='2',
            ['3']='3',
            ['4']='4',
            ['5']='5',
            ['5']='5',
            ['6']='6',
            ['7']='7',
            ['8']='8',
            ['9']='9',
        };
        


        private static bool IsSpace(this byte c)
        {
            return c <= 32;
        }

        private static bool IsStartOfBool(this byte c)
        {
            return c == (byte) 't' || c == (byte) 'f' || c == (byte) 'T' || c == (byte) 'F';
        }



        private static void ThrowException(this byte[] byteArray, int position)
        {
            throw new Exception("Invalid Json at position: "+position);
        }

        public static IEnumerable<(KeyValuePair<int, int> field, KeyValuePair<int, int> value)> ParseFirstLevelOfJson(this byte[] byteArray)
        {
            var expectedToken = ExpectedToken.OpenBracket;

      

            var start = 0;
            var subObjectLevel = 0;
            var subObjectString = false;

            KeyValuePair<int, int> field = new KeyValuePair<int, int>(0,0);
            
            for (var i = 0; i < byteArray.Length; i++)
            {
                var c = byteArray[i];
                if (expectedToken == ExpectedToken.EndOfFile)
                    break;
                
                switch (expectedToken)
                {
                    case ExpectedToken.OpenBracket:
                        if (c.IsSpace())
                            continue;
                        if (c != OpenBracket)
                            byteArray.ThrowException(i);

                        expectedToken = ExpectedToken.OpenKey;
                        break;
                    
                    case ExpectedToken.OpenKey:
                        if (c == CloseBraked)
                        {
                            expectedToken = ExpectedToken.EndOfFile;
                            break;
                        }
                        
                        if (c.IsSpace())
                            continue;
                        
                        if (c != DoubleQuote)
                            byteArray.ThrowException(i);
                        
                        start = i;
                        expectedToken = ExpectedToken.CloseKey;
                        break;
                    
                    case ExpectedToken.CloseKey:
                        if (c == EscSymbol)
                        {
                            i++;
                            continue;
                        }

                        if (c == DoubleQuote)
                        {
                            field = new KeyValuePair<int, int>(start, i+1);
                            expectedToken = ExpectedToken.DoubleColumn;
                        }
                        break;
                    
                    case ExpectedToken.DoubleColumn:
                        if (c.IsSpace())
                            continue;
                        
                        if (c != DoubleColumn)
                            byteArray.ThrowException(i);
                        
                        expectedToken = ExpectedToken.OpenValue;                        
                        break;
                    
                    case ExpectedToken.OpenValue:
                        if (c.IsSpace())
                            continue;

                        start = i;
                        
                        switch (c)
                        {
                            case OpenArray:
                                expectedToken = ExpectedToken.CloseArray;
                                break;
                            case DoubleQuote:
                                expectedToken = ExpectedToken.CloseStringValue;
                                break;
                            case OpenBracket:
                                subObjectLevel = 0;
                                subObjectString = false;
                                expectedToken = ExpectedToken.CloseObject;
                                break;
                            default:
                            {
                                if (StartOfDigit.ContainsKey((char)c) || c.IsStartOfBool())
                                    expectedToken = ExpectedToken.CloseNumberOrBoolValue;
                                else
                                    byteArray.ThrowException(i);

                                break;
                            }
                        }
                        break;
                    
                    case ExpectedToken.CloseStringValue:
                        if (c == EscSymbol)
                        {
                            i++;
                            continue;
                        }

                        if (c == DoubleQuote)
                        {
                            yield return (field, new KeyValuePair<int, int>(start, i+1));
                            expectedToken = ExpectedToken.Comma;
                        }
                        break;
                    
                    case ExpectedToken.CloseNumberOrBoolValue:
                        if (c == Comma || c.IsSpace())
                        {
                            yield return (field, new KeyValuePair<int, int>(start, i+1));
                            expectedToken = c == Comma ? ExpectedToken.OpenKey : ExpectedToken.Comma;
                        }
                        break;
                    
                    case ExpectedToken.Comma:
                        if (c.IsSpace())
                        continue;
                        if (c == CloseBraked)
                        {
                            expectedToken = ExpectedToken.EndOfFile;
                            continue;
                        }
                        
                        if (c != Comma)
                            byteArray.ThrowException(i);
                       
                            expectedToken = ExpectedToken.OpenKey;
                        continue;
                    
                    case ExpectedToken.CloseObject:
                        if (subObjectString)
                        {
                            switch (c)
                            {
                                case EscSymbol:
                                    i++;
                                    continue;
                                case DoubleQuote:
                                    subObjectString = false;
                                    break;
                            }
                        }
                        else
                        {
                            switch (c)
                            {
                                case DoubleQuote:
                                    subObjectString = true;
                                    continue;
                                case OpenBracket:
                                    subObjectLevel++;
                                    continue;
                                case CloseBraked when subObjectLevel == 0:
                                    yield return (field, new KeyValuePair<int, int>(start, i+1));
                                    expectedToken = ExpectedToken.Comma;
                                    break;
                                case CloseBraked:
                                    subObjectLevel--;
                                    break;
                            }
                        }
                        
                        
                        break;
                    
                    case ExpectedToken.CloseArray:
                        if (subObjectString)
                        {
                            switch (c)
                            {
                                case EscSymbol:
                                    i++;
                                    continue;
                                case DoubleQuote:
                                    subObjectString = false;
                                    break;
                            }
                        }
                        else
                        {
                            switch (c)
                            {
                                case DoubleQuote:
                                    subObjectString = true;
                                    continue;
                                case OpenArray:
                                    subObjectLevel++;
                                    continue;
                                case CloseArray when subObjectLevel == 0:
                                    yield return (field, new KeyValuePair<int, int>(start, i+1));
                                    expectedToken = ExpectedToken.Comma;
                                    break;
                                case CloseArray:
                                    subObjectLevel--;
                                    break;
                            }
                        }
                        
                        
                        break;                    
                        
                }
       
            }
            
            if (expectedToken != ExpectedToken.EndOfFile)
                throw new Exception("Invalid Json");
        }
        
    }
}