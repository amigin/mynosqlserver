using System;
using System.Collections.Generic;
using System.IO;

namespace Common
{

    
    
    public class ChunkedStream : Stream
    {

        private readonly List<ArraySpan<byte>> _streamData = new List<ArraySpan<byte>>();
        
        public override void Flush()
        {
            
        }
        
        private int CopyToBuffer(int position, byte[] buffer, int offset, int count)
        {
            var (chunkIndex, chunkOffset) = CalcMemoryPosition(position);

            var result = 0;
            
            while (offset+result < count)
            {
                var chunk = _streamData[chunkIndex];

                var copySize = count-result;

                if (copySize > chunk.Length - chunkOffset)
                    copySize = chunk.Length - chunkOffset;
                
                chunk.CopyToArray(chunkOffset, buffer, result+offset, copySize);

                chunkIndex++;
                chunkOffset = 0;
                result += copySize;

            }


            Position += result;
            return result;
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var len = (int)(_length - Position);

            if (len == 0)
                return 0;
            
            if (count > len)
                count = len;
            
            return CopyToBuffer((int) Position, buffer, offset, count);
        }


        public byte[] AsArray()
        {
            var result = new byte[Length];
            CopyToBuffer(0, result, 0, result.Length);
            return result;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            if (count == 0)
                return;
            
            _streamData.Add(buffer.ToByteArraySpan(offset, count));
            _length += count;
        }
        
        public void Write(ArraySpan<byte> arraySpan)
        {
            _streamData.Add(arraySpan);
            _length += arraySpan.Length;
        }

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => true;

        private long _length;
        public override long Length => _length;

        private (int chunkIndex, int offset) CalcMemoryPosition(int position)
        {

            
            for (var chunkIndex = 0; chunkIndex < _streamData.Count; chunkIndex++)
            {
                if (position < _streamData[chunkIndex].Length)
                    return (chunkIndex, position);

                position -= _streamData[chunkIndex].Length;
            }
            
            throw new IndexOutOfRangeException($"Index {position} is out of range of range of the stream");
        }

        public override long Position { get; set; }
    }
}