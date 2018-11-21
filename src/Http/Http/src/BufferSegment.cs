using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;

namespace Microsoft.AspNetCore.Http
{
    public class BufferSegment : ReadOnlySequenceSegment<byte>
    {
        private IMemoryOwner<byte> _memoryOwner;
        private BufferSegment _next;
        private int _end;

        public int Start { get; private set; }

        public int End
        {
            get => _end;
            set
            {
                _end = value;
                Memory = AvailableMemory.Slice(Start, _end - Start);

            }
        }

        public BufferSegment NextSegment
        {
            get => _next;
            set
            {
                _next = value;
                Next = value;
            }
        }

        public void SetMemory(IMemoryOwner<byte> memoryOwner)
        {
            SetMemory(memoryOwner, 0, 0);
        }

        public void SetMemory(IMemoryOwner<byte> memoryOwner, int start, int end, bool readOnly = false)
        {
            _memoryOwner = memoryOwner;

            AvailableMemory = _memoryOwner.Memory;

            ReadOnly = readOnly;
            RunningIndex = 0;
            Start = start;
            End = end;
            NextSegment = null;
        }

        public void ResetMemory()
        {
            _memoryOwner.Dispose();
            _memoryOwner = null;
            AvailableMemory = default;
        }

        internal IMemoryOwner<byte> MemoryOwner => _memoryOwner;

        public Memory<byte> AvailableMemory { get; private set; }

        public int Length => End - Start;

        public bool ReadOnly { get; private set; }

        public int WritableBytes
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => AvailableMemory.Length - End;
        }

        public void SetNext(BufferSegment segment)
        {
            NextSegment = segment;

            segment = this;

            while (segment.Next != null)
            {
                segment.NextSegment.RunningIndex = segment.RunningIndex + segment.Length;
                segment = segment.NextSegment;
            }
        }
    }
}
