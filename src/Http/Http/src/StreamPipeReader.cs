// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Buffers;
using System.IO;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.AspNetCore.Http
{
    public class StreamPipeReader : PipeReader
    {
        private readonly int _minimumSegmentSize;
        private readonly int _minimumReadSize;
        private readonly Stream _readingStream;
        private readonly MemoryPool<byte> _pool;

        private CancellationTokenSource _internalTokenSource;
        private bool _isCompleted;
        private ExceptionDispatchInfo _exceptionInfo;
        private object lockObject = new object();

        private BufferSegment _readHead;
        private int _readIndex;

        private BufferSegment _commitHead;
        private long _consumedLength;
        private bool _examinedEverything;

        private static readonly ReadOnlySequence<byte> EmptySequence = new ReadOnlySequence<byte>(new BufferSegment(), 0, new BufferSegment(), 0);

        private CancellationTokenSource InternalTokenSource
        {
            get
            {
                lock (lockObject)
                {
                    if (_internalTokenSource == null)
                    {
                        _internalTokenSource = new CancellationTokenSource();
                    }
                    return _internalTokenSource;
                }
            }
        }

        public StreamPipeReader(Stream readingStream) : this(readingStream, 4096)
        {
        }

        public StreamPipeReader(Stream readingStream, int minimumSegmentSize, MemoryPool<byte> pool = null)
        {
            _minimumSegmentSize = minimumSegmentSize;
            _minimumReadSize = _minimumSegmentSize / 4;
            _readingStream = readingStream;
            _pool = pool ?? MemoryPool<byte>.Shared;
        }

        public override void AdvanceTo(SequencePosition consumed)
        {
            AdvanceTo(consumed, consumed);
        }

        public override void AdvanceTo(SequencePosition consumed, SequencePosition examined)
        {
            AdvanceTo((BufferSegment)consumed.GetObject(), consumed.GetInteger(), (BufferSegment)examined.GetObject(), examined.GetInteger());
        }

        private void AdvanceTo(BufferSegment consumedSegment, int consumedIndex, BufferSegment examinedSegment, int examinedIndex)
        {
            if (_isCompleted)
            {
                throw new InvalidOperationException("Reading is not allowed after reader was completed.");
            }

            if (consumedSegment == null)
            {
                return;
            }

            // if there is a consumed segment and we haven't read anything, we are in some invalid state
            if (_readHead == null || _commitHead == null)
            {
                // TODO these exception messages 
                throw new InvalidOperationException("Pipe is already advanced past provided cursor.");
            }

            // All of the bytes we need to return
            var returnStart = _readHead;
            var returnEnd = consumedSegment;
                
            // need to figure out how far we consumed and examined.
            var consumedBytes = new ReadOnlySequence<byte>(returnStart, _readIndex, consumedSegment, consumedIndex).Length;
            if (_consumedLength - consumedBytes < 0)
            {
                throw new InvalidOperationException("Pipe is already advanced past provided cursor.");
            }

            _consumedLength -= consumedBytes;

            if (examinedSegment == _commitHead)
            {
                _examinedEverything = _commitHead != null ? examinedIndex == _commitHead.End - _commitHead.Start : examinedIndex == 0;
            }

            // Three cases here:
            // 1. All data is consumed. If so, we clear _readHead/_commitHead and _readIndex/
            //  returnEnd is set to null to free all memory between returnStart/End
            // 2. A segment is entirely consumed but there is still more data in nextSegments
            //  We are allowed to remove an extra segment. by setting returnEnd to be the next block.
            // 3. We are in the middle of a segment.
            //  Move _readHead and _readIndex to consumedSegment and index
            if (_consumedLength == 0)
            {
                _readHead = null;
                _commitHead = null;
                returnEnd = null;
                _readIndex = 0;
            }
            else if (consumedIndex == returnEnd.Length)
            {
                var nextBlock = returnEnd.NextSegment;
                _readHead = nextBlock;
                _readIndex = 0;
                returnEnd = nextBlock;
            }
            else
            {
                _readHead = consumedSegment;
                _readIndex = consumedIndex;
            }

            while (returnStart != null && returnStart != returnEnd)
            {
                returnStart.ResetMemory();
                returnStart = returnStart.NextSegment;
            }
        }

        public override void CancelPendingRead()
        {
            InternalTokenSource.Cancel();
        }

        public override void Complete(Exception exception = null)
        {
            if (_isCompleted)
            {
                return;
            }

            _isCompleted = true;
            if (exception != null)
            {
                _exceptionInfo = ExceptionDispatchInfo.Capture(exception);
            }

            var segment = _readHead;
            while (segment != null)
            {
                segment.ResetMemory();
                segment = segment.NextSegment;
            }
        }

        public override void OnWriterCompleted(Action<Exception, object> callback, object state)
        {
            throw new NotSupportedException();
        }

        public override async ValueTask<ReadResult> ReadAsync(CancellationToken cancellationToken = default)
        {
            if (_isCompleted)
            {
                throw new InvalidOperationException("Reading is not allowed after reader was completed.");
            }

            if (TryRead(out var readResult))
            {
                return readResult;
            }

            var reg = new CancellationTokenRegistration();
            if (cancellationToken.CanBeCanceled)
            {
                reg = cancellationToken.Register(state => ((StreamPipeReader)state).Cancel(), this);
            }
            using (reg)
            {
                try
                {
                    AllocateCommitHead();
#if NETCOREAPP2_2
                    var length = await _readingStream.ReadAsync(_commitHead.AvailableMemory, InternalTokenSource.Token);
#elif NETSTANDARD2_0
                    MemoryMarshal.TryGetArray<byte>(_commitHead.AvailableMemory, out var arraySegment);
                    var length = await _readingStream.ReadAsync(arraySegment.Array, 0, arraySegment.Count, InternalTokenSource.Token);
#else
#error Target frameworks need to be updated.
#endif
                    _commitHead.End += length;
                    _consumedLength += length;

                    var ros = new ReadOnlySequence<byte>(_readHead, _readIndex, _commitHead, _commitHead.End - _commitHead.Start);
                    return new ReadResult(ros, isCanceled: false, IsCompletedOrThrow());
                }
                catch (OperationCanceledException)
                {
                    // Remove the cancellation token such that the next time Flush is called
                    // A new CTS is created.
                    lock (lockObject)
                    {
                        _internalTokenSource = null;
                    }

                    if (cancellationToken.IsCancellationRequested)
                    {
                        throw;
                    }

                    // Catch any cancellation and translate it into setting isCanceled = true
                    var ros = new ReadOnlySequence<byte>(_readHead, _readIndex, _commitHead, _commitHead.End - _commitHead.Start);
                    return new ReadResult(ros, isCanceled: true, IsCompletedOrThrow());
                }
            }
        }

        private void AllocateCommitHead()
        {
            BufferSegment segment;
            if (_commitHead != null)
            {
                segment = _commitHead;
                var bytesLeftInBuffer = segment.WritableBytes;
                if (bytesLeftInBuffer == 0 || bytesLeftInBuffer < _minimumReadSize || segment.ReadOnly)
                {
                    var nextSegment = CreateSegmentUnsynchronized();
                    nextSegment.SetMemory(_pool.Rent(GetSegmentSize()));
                    segment.SetNext(nextSegment);
                    _commitHead = nextSegment;
                }
            }
            else
            {
                if (_readHead != null && !_commitHead.ReadOnly)
                {
                    // Don't believe this can be hit.
                    var remaining = _commitHead.WritableBytes;
                    if (_minimumReadSize <= remaining && remaining > 0)
                    {
                        segment = _readHead;
                        _commitHead = segment;
                        return;
                    }
                }

                segment = CreateSegmentUnsynchronized();
                segment.SetMemory(_pool.Rent(GetSegmentSize()));
                if (_readHead == null)
                {
                    _readHead = segment;
                }
                else if (segment != _readHead && _readHead.Next == null)
                {
                    _readHead.SetNext(segment);
                }

                _commitHead = segment;
            }
        }

        private int GetSegmentSize()
        {
            var adjustedToMaximumSize = Math.Min(_pool.MaxBufferSize, _minimumSegmentSize);
            return adjustedToMaximumSize;
        }

        private BufferSegment CreateSegmentUnsynchronized()
        {
            // TODO this can pool
            return new BufferSegment();
        }
            
        public override bool TryRead(out ReadResult result)
        {
            if (InternalTokenSource.IsCancellationRequested)
            {
                AllocateCommitHead();

                lock (lockObject)
                {
                    _internalTokenSource = null;
                }

                result = new ReadResult(
                    new ReadOnlySequence<byte>(_readHead, _readIndex, _commitHead, _commitHead.End - _commitHead.Start),
                    isCanceled: true, 
                    IsCompletedOrThrow());

                return true;
            }

            if (_consumedLength > 0 && !_examinedEverything)
            {
                var ros = new ReadOnlySequence<byte>(_readHead, _readIndex, _commitHead, _commitHead.End - _commitHead.Start);
                result = new ReadResult(ros, isCanceled: false, IsCompletedOrThrow());
                return true;
            }

            result = new ReadResult();
            return false;
        }

        private void Cancel()
        {
            InternalTokenSource.Cancel();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool IsCompletedOrThrow()
        {
            if (!_isCompleted)
            {
                return false;
            }
            if (_exceptionInfo != null)
            {
                ThrowLatchedException();
            }
            return true;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void ThrowLatchedException()
        {
            _exceptionInfo.Throw();
        }

        public void Dispose()
        {
            Complete();
        }
    }
}
