using System;
using System.Collections.Generic;
using System.Text;

namespace MyNoSqlServer.Domains
{

    public enum ExpectedToken
    {
        OpenBracket, OpenKey, CloseKey, DoubleColumn, OpenValue, CloseStringValue, CloseNumberOrBoolValue, CloseObject, CloseArray, Comma, EndOfFile
    }

    
    
    public static class JsonByteArrayReader
    {
        
        public const byte OpenBracket = (byte) '{';
        public const byte CloseBracket = (byte) '}';
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
            return c == (byte) 't' || c == (byte) 'f' || c == (byte) 'T' || c == (byte) 'F' || c == (byte)'n' || c == (byte)'N';
        }


        private static void ThrowException(this byte[] byteArray, int position)
        {
            var i = position - 10;
            if (i < 0)
                i = 0;

            var str = Encoding.UTF8.GetString(byteArray, i, position - i);

            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine(str);
            Console.ResetColor();
            
            throw new Exception("Invalid Json at position: "+str);
        }

        public static IEnumerable<(ArraySpan field, ArraySpan value)> ParseFirstLevelOfJson(this byte[] byteArray)
        {
            var expectedToken = ExpectedToken.OpenBracket;

      

            var subObjectLevel = 0;
            var subObjectString = false;

            ArraySpan keyField = null;
            ArraySpan valueField = null;
            
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
                        if (c == CloseBracket)
                        {
                            expectedToken = ExpectedToken.EndOfFile;
                            break;
                        }
                        
                        if (c.IsSpace())
                            continue;
                        
                        if (c != DoubleQuote)
                            byteArray.ThrowException(i);
                        
                        keyField = new ArraySpan(byteArray){StartIndex = i};
                        expectedToken = ExpectedToken.CloseKey;
                        break;
                    
                    case ExpectedToken.CloseKey:
                        switch (c)
                        {
                            case EscSymbol:
                                i++;
                                break;
                            case DoubleQuote:
                                if (keyField != null)
                                  keyField.EndIndex = i+1;
                                expectedToken = ExpectedToken.DoubleColumn;
                                break;
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

                        valueField = new ArraySpan(byteArray){StartIndex = i};
                        
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
                        switch (c)
                        {
                            case EscSymbol:
                                i++;
                                break;
                            case DoubleQuote:
                                if (valueField != null)
                                  valueField.EndIndex = i + 1;
                                yield return (keyField, valueField);
                                expectedToken = ExpectedToken.Comma;
                                break;
                        }

                        break;
                    
                    case ExpectedToken.CloseNumberOrBoolValue:
                        if (c == Comma || c == CloseBracket || c.IsSpace())
                        {
                            if (valueField != null)
                              valueField.EndIndex = i;
                            yield return (keyField, valueField);
                            if (c == CloseBracket)
                                expectedToken = ExpectedToken.EndOfFile;
                            else
                            expectedToken = c == Comma ? ExpectedToken.OpenKey : ExpectedToken.Comma;
                        }
                        break;
                    
                    case ExpectedToken.Comma:
                        if (c.IsSpace())
                        continue;
                        if (c == CloseBracket)
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
                                case CloseBracket when subObjectLevel == 0:
                                    if (valueField != null)
                                      valueField.EndIndex = i + 1;
                                    yield return (keyField, valueField);
                                    expectedToken = ExpectedToken.Comma;
                                    break;
                                case CloseBracket:
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
                                    if (valueField != null)
                                      valueField.EndIndex = i + 1;
                                    yield return (keyField, valueField);
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
        
        
        
        public static IEnumerable<ArraySpan> SplitJsonArrayToObjects(this byte[] byteArray)
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
                                    EndIndex = i+1
                                };  
                        }

                        break;
                }

            }

        }
    }
}