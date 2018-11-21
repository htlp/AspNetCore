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
    /// <summary>
    /// Implements PipeReader using an underlying stream.
    /// </summary>
    public class StreamPipeReader : PipeReader
    {
        private readonly int _minimumSegmentSize;
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

        /// <summary>
        /// Creates a new StreamPipeReader.
        /// </summary>
        /// <param name="readingStream">The stream to read from.</param>
        public StreamPipeReader(Stream readingStream) : this(readingStream, minimumSegmentSize: 4096)
        {
        }

        /// <summary>
        /// Creates a new StreamPipeReader.
        /// </summary>
        /// <param name="readingStream">The stream to read from.</param>
        /// <param name="minimumSegmentSize">The minimum segment size to return from ReadAsync.</param>
        /// <param name="pool"></param>
        public StreamPipeReader(Stream readingStream, int minimumSegmentSize, MemoryPool<byte> pool = null)
        {
            _minimumSegmentSize = minimumSegmentSize;
            _readingStream = readingStream;
            _pool = pool ?? MemoryPool<byte>.Shared;
        }

        /// <inheritdoc />
        public override void AdvanceTo(SequencePosition consumed)
        {
            AdvanceTo(consumed, consumed);
        }

        /// <inheritdoc />
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

            if (_readHead == null || _commitHead == null)
            {
                throw new InvalidOperationException("Pipe is already advanced past provided cursor.");
            }

            var returnStart = _readHead;
            var returnEnd = consumedSegment;
                
            var consumedBytes = new ReadOnlySequence<byte>(returnStart, _readIndex, consumedSegment, consumedIndex).Length;
            if (_consumedLength - consumedBytes < 0)
            {
                throw new InvalidOperationException("Pipe is already advanced past provided cursor.");
            }

            _consumedLength -= consumedBytes;

            if (examinedSegment == _commitHead)
            {
                // If we examined everything, we force ReadAsync to actually read from the underlying stream
                // instead of returning a ReadResult from TryRead.
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

            // Remove all blocks that 
            while (returnStart != null && returnStart != returnEnd)
            {
                returnStart.ResetMemory();
                returnStart = returnStart.NextSegment;
            }
        }

        /// <inheritdoc />
        public override void CancelPendingRead()
        {
            InternalTokenSource.Cancel();
        }

        /// <inheritdoc />
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

        /// <inheritdoc />
        public override void OnWriterCompleted(Action<Exception, object> callback, object state)
        {
            throw new NotSupportedException();
        }

        /// <inheritdoc />
        public override async ValueTask<ReadResult> ReadAsync(CancellationToken cancellationToken = default)
        {
            if (_isCompleted)
            {
                throw new InvalidOperationException("Reading is not allowed after reader was completed.");
            }

            // PERF: store InternalTokenSource locally to avoid querying it twice (which acquires a lock)
            var tokenSource = InternalTokenSource;
            if (TryReadInternal(tokenSource, out var readResult))
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
                    var length = await _readingStream.ReadAsync(_commitHead.AvailableMemory, tokenSource.Token);
#elif NETSTANDARD2_0
                    MemoryMarshal.TryGetArray<byte>(_commitHead.AvailableMemory, out var arraySegment);
                    var length = await _readingStream.ReadAsync(arraySegment.Array, 0, arraySegment.Count, tokenSource.Token);
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
                    lock (lockObject)
                    {
                        _internalTokenSource = null;
                    }

                    if (cancellationToken.IsCancellationRequested)
                    {
                        throw;
                    }

                    var ros = new ReadOnlySequence<byte>(_readHead, _readIndex, _commitHead, _commitHead.End - _commitHead.Start);
                    return new ReadResult(ros, isCanceled: true, IsCompletedOrThrow());
                }
            }
        }

        public override bool TryRead(out ReadResult result)
        {
            return TryReadInternal(InternalTokenSource, out result);
        }

        private bool TryReadInternal(CancellationTokenSource source, out ReadResult result)
        {
            if (source.IsCancellationRequested)
            {
                // If TryRead/ReadAsync are called and cancellation is requested, we need to make sure memory is allocated for the ReadResult,
                // otherwise if someone calls advance afterward on the ReadResult, it will throw.
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

        private void AllocateCommitHead()
        {
            BufferSegment segment;
            if (_commitHead != null)
            {
                segment = _commitHead;
                var bytesLeftInBuffer = segment.WritableBytes;
                // Check if we need create a new segment (if we need more data to read)
                if (bytesLeftInBuffer == 0 || segment.ReadOnly)
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
                    var remaining = _commitHead.WritableBytes;
                    // If there is enough bytes remaining, we don't need to allocate a new segment.
                    if (remaining > 0)
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
            return new BufferSegment();
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
