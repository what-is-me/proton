#include <Common/ProfileEvents.h>
#include <Common/CurrentThread.h>

/// Available events. Add something here as you wish.
#define APPLY_FOR_EVENTS(M) \
    M(Query, "Number of queries to be interpreted and potentially executed. Does not include queries that failed to parse or were rejected due to AST size limits, quota limits or limits on the number of simultaneously running queries. May include internal queries initiated by proton itself. Does not count subqueries.") \
    M(SelectQuery, "Same as Query, but only for SELECT queries.") \
    M(StreamingSelectQuery, "Same as Query, but only for streaming SELECT queries.") \
    M(HistoricalSelectQuery, "Same as Query, but only for historical SELECT queries.") \
    M(InsertQuery, "Same as Query, but only for INSERT queries.") \
    M(FailedQuery, "Number of failed queries.") \
    M(FailedSelectQuery, "Same as FailedQuery, but only for SELECT queries.") \
    M(FailedInsertQuery, "Same as FailedQuery, but only for INSERT queries.") \
    M(QueryTimeMicroseconds, "Total time of all queries.") \
    M(SelectQueryTimeMicroseconds, "Total time of SELECT queries.") \
    M(FileOpen, "Number of files opened.") \
    M(Seek, "Number of times the 'lseek' function was called.") \
    M(ReadBufferFromFileDescriptorRead, "Number of reads (read/pread) from a file descriptor. Does not include sockets.") \
    M(ReadBufferFromFileDescriptorReadFailed, "Number of times the read (read/pread) from a file descriptor have failed.") \
    M(ReadBufferFromFileDescriptorReadBytes, "Number of bytes read from file descriptors. If the file is compressed, this will show the compressed data size.") \
    M(WriteBufferFromFileDescriptorWrite, "Number of writes (write/pwrite) to a file descriptor. Does not include sockets.") \
    M(WriteBufferFromFileDescriptorWriteFailed, "Number of times the write (write/pwrite) to a file descriptor have failed.") \
    M(WriteBufferFromFileDescriptorWriteBytes, "Number of bytes written to file descriptors. If the file is compressed, this will show compressed data size.") \
    M(ReadCompressedBytes, "Number of bytes (the number of bytes before decompression) read from compressed sources (files, network).") \
    M(CompressedReadBufferBlocks, "Number of compressed blocks (the blocks of data that are compressed independent of each other) read from compressed sources (files, network).") \
    M(CompressedReadBufferBytes, "Number of uncompressed bytes (the number of bytes after decompression) read from compressed sources (files, network).") \
    M(UncompressedCacheHits, "") \
    M(UncompressedCacheMisses, "") \
    M(UncompressedCacheWeightLost, "") \
    M(MMappedFileCacheHits, "") \
    M(MMappedFileCacheMisses, "") \
    M(OpenedFileCacheHits, "") \
    M(OpenedFileCacheMisses, "") \
    M(AIOWrite, "Number of writes with Linux or FreeBSD AIO interface") \
    M(AIOWriteBytes, "Number of bytes written with Linux or FreeBSD AIO interface") \
    M(AIORead, "Number of reads with Linux or FreeBSD AIO interface") \
    M(AIOReadBytes, "Number of bytes read with Linux or FreeBSD AIO interface") \
    M(IOBufferAllocs, "") \
    M(IOBufferAllocBytes, "") \
    M(ArenaAllocChunks, "") \
    M(ArenaAllocBytes, "") \
    M(FunctionExecute, "") \
    M(TableFunctionExecute, "") \
    M(MarkCacheHits, "") \
    M(MarkCacheMisses, "") \
    M(CreatedReadBufferOrdinary, "") \
    M(CreatedReadBufferDirectIO, "") \
    M(CreatedReadBufferDirectIOFailed, "") \
    M(CreatedReadBufferMMap, "") \
    M(CreatedReadBufferMMapFailed, "") \
    M(DiskReadElapsedMicroseconds, "Total time spent waiting for read syscall. This include reads from page cache.") \
    M(DiskWriteElapsedMicroseconds, "Total time spent waiting for write syscall. This include writes to page cache.") \
    M(NetworkReceiveElapsedMicroseconds, "Total time spent waiting for data to receive or receiving data from network. Only proton-related network interaction is included, not by 3rd party libraries.") \
    M(NetworkSendElapsedMicroseconds, "Total time spent waiting for data to send to network or sending data to network. Only proton-related network interaction is included, not by 3rd party libraries..") \
    M(NetworkReceiveBytes, "Total number of bytes received from network. Only proton-related network interaction is included, not by 3rd party libraries.") \
    M(NetworkSendBytes, "Total number of bytes send to network. Only proton-related network interaction is included, not by 3rd party libraries.") \
    \
    M(DiskS3GetRequestThrottlerCount, "Number of DiskS3 GET and SELECT requests passed through throttler.") \
    M(DiskS3GetRequestThrottlerSleepMicroseconds, "Total time a query was sleeping to conform DiskS3 GET and SELECT request throttling.") \
    M(DiskS3PutRequestThrottlerCount, "Number of DiskS3 PUT, COPY, POST and LIST requests passed through throttler.") \
    M(DiskS3PutRequestThrottlerSleepMicroseconds, "Total time a query was sleeping to conform DiskS3 PUT, COPY, POST and LIST request throttling.") \
    \
    M(ThrottlerSleepMicroseconds, "Total time a query was sleeping to conform 'max_network_bandwidth' and other throttling settings.") \
    \
    M(QueryMaskingRulesMatch, "Number of times query masking rules was successfully matched.") \
    \
    M(ReplicatedPartFetches, "Number of times a data part was downloaded from replica of a ReplicatedMergeTree table.") \
    M(ReplicatedPartFailedFetches, "Number of times a data part was failed to download from replica of a ReplicatedMergeTree table.") \
    M(ObsoleteReplicatedParts, "") \
    M(ReplicatedPartMerges, "Number of times data parts of ReplicatedMergeTree tables were successfully merged.") \
    M(ReplicatedPartFetchesOfMerged, "Number of times we prefer to download already merged part from replica of ReplicatedMergeTree table instead of performing a merge ourself (usually we prefer doing a merge ourself to save network traffic). This happens when we have not all source parts to perform a merge or when the data part is old enough.") \
    M(ReplicatedPartMutations, "") \
    M(ReplicatedPartChecks, "") \
    M(ReplicatedPartChecksFailed, "") \
    M(ReplicatedDataLoss, "Number of times a data part that we wanted doesn't exist on any replica (even on replicas that are offline right now). That data parts are definitely lost. This is normal due to asynchronous replication (if quorum inserts were not enabled), when the replica on which the data part was written was failed and when it became online after fail it doesn't contain that data part.") \
    \
    M(InsertedRows, "Number of rows INSERTed to all tables.") \
    M(InsertedBytes, "Number of bytes (uncompressed; for columns as they stored in memory) INSERTed to all tables.") \
    M(DelayedInserts, "Number of times the INSERT of a block to a MergeTree table was throttled due to high number of active data parts for partition.") \
    M(RejectedInserts, "Number of times the INSERT of a block to a MergeTree table was rejected with 'Too many parts' exception due to high number of active data parts for partition.") \
    M(DelayedInsertsMilliseconds, "Total number of milliseconds spent while the INSERT of a block to a MergeTree table was throttled due to high number of active data parts for partition.") \
    M(DistributedDelayedInserts, "Number of times the INSERT of a block to a Distributed table was throttled due to high number of pending bytes.") \
    M(DistributedRejectedInserts, "Number of times the INSERT of a block to a Distributed table was rejected with 'Too many bytes' exception due to high number of pending bytes.") \
    M(DistributedDelayedInsertsMilliseconds, "Total number of milliseconds spent while the INSERT of a block to a Distributed table was throttled due to high number of pending bytes.") \
    M(DuplicatedInsertedBlocks, "Number of times the INSERTed block to a ReplicatedMergeTree table was deduplicated.") \
    \
    M(ZooKeeperInit, "") \
    M(ZooKeeperTransactions, "") \
    M(ZooKeeperList, "") \
    M(ZooKeeperCreate, "") \
    M(ZooKeeperRemove, "") \
    M(ZooKeeperExists, "") \
    M(ZooKeeperGet, "") \
    M(ZooKeeperSet, "") \
    M(ZooKeeperMulti, "") \
    M(ZooKeeperCheck, "") \
    M(ZooKeeperClose, "") \
    M(ZooKeeperWatchResponse, "") \
    M(ZooKeeperUserExceptions, "") \
    M(ZooKeeperHardwareExceptions, "") \
    M(ZooKeeperOtherExceptions, "") \
    M(ZooKeeperWaitMicroseconds, "") \
    M(ZooKeeperBytesSent, "") \
    M(ZooKeeperBytesReceived, "") \
    \
    M(DistributedConnectionFailTry, "Total count when distributed connection fails with retry") \
    M(DistributedConnectionMissingTable, "") \
    M(DistributedConnectionStaleReplica, "") \
    M(DistributedConnectionFailAtAll, "Total count when distributed connection fails after all retries finished") \
    \
    M(HedgedRequestsChangeReplica, "Total count when timeout for changing replica expired in hedged requests.") \
    \
    M(CompileFunction, "Number of times a compilation of generated LLVM code (to create fused function for complex expressions) was initiated.") \
    M(CompiledFunctionExecute, "Number of times a compiled function was executed.") \
    M(CompileExpressionsMicroseconds, "Total time spent for compilation of expressions to LLVM code.") \
    M(CompileExpressionsBytes, "Number of bytes used for expressions compilation.") \
    \
    M(ExternalSortWritePart, "") \
    M(ExternalSortMerge, "") \
    M(ExternalAggregationWritePart, "") \
    M(ExternalAggregationMerge, "") \
    M(ExternalAggregationCompressedBytes, "") \
    M(ExternalAggregationUncompressedBytes, "") \
    \
    M(SlowRead, "Number of reads from a file that were slow. This indicate system overload. Thresholds are controlled by read_backoff_* settings.") \
    M(ReadBackoff, "Number of times the number of query processing threads was lowered due to slow reads.") \
    \
    M(ReplicaPartialShutdown, "How many times Replicated table has to deinitialize its state due to session expiration in ZooKeeper. The state is reinitialized every time when ZooKeeper is available again.") \
    \
    M(SelectedParts, "Number of data parts selected to read from a MergeTree table.") \
    M(SelectedRanges, "Number of (non-adjacent) ranges in all data parts selected to read from a MergeTree table.") \
    M(SelectedMarks, "Number of marks (index granules) selected to read from a MergeTree table.") \
    M(SelectedRows, "Number of rows SELECTed from all tables.") \
    M(SelectedBytes, "Number of bytes (uncompressed; for columns as they stored in memory) SELECTed from all tables.") \
    \
    M(WaitMarksLoadMicroseconds, "Time spent loading marks") \
    M(BackgroundLoadingMarksTasks, "Number of background tasks for loading marks") \
    \
    M(Merge, "Number of launched background merges.") \
    M(MergedRows, "Rows read for background merges. This is the number of rows before merge.") \
    M(MergedUncompressedBytes, "Uncompressed bytes (for columns as they stored in memory) that was read for background merges. This is the number before merge.") \
    M(MergesTimeMilliseconds, "Total time spent for background merges.")\
    \
    M(MergeTreeDataWriterRows, "Number of rows INSERTed to MergeTree tables.") \
    M(MergeTreeDataWriterUncompressedBytes, "Uncompressed bytes (for columns as they stored in memory) INSERTed to MergeTree tables.") \
    M(MergeTreeDataWriterCompressedBytes, "Bytes written to filesystem for data INSERTed to MergeTree tables.") \
    M(MergeTreeDataWriterBlocks, "Number of blocks INSERTed to MergeTree tables. Each block forms a data part of level zero.") \
    M(MergeTreeDataWriterBlocksAlreadySorted, "Number of blocks INSERTed to MergeTree tables that appeared to be already sorted.") \
    \
    M(MergeTreeDataProjectionWriterRows, "Number of rows INSERTed to MergeTree tables projection.") \
    M(MergeTreeDataProjectionWriterUncompressedBytes, "Uncompressed bytes (for columns as they stored in memory) INSERTed to MergeTree tables projection.") \
    M(MergeTreeDataProjectionWriterCompressedBytes, "Bytes written to filesystem for data INSERTed to MergeTree tables projection.") \
    M(MergeTreeDataProjectionWriterBlocks, "Number of blocks INSERTed to MergeTree tables projection. Each block forms a data part of level zero.") \
    M(MergeTreeDataProjectionWriterBlocksAlreadySorted, "Number of blocks INSERTed to MergeTree tables projection that appeared to be already sorted.") \
    \
    M(CannotRemoveEphemeralNode, "Number of times an error happened while trying to remove ephemeral node. This is not an issue, because our implementation of ZooKeeper library guarantee that the session will expire and the node will be removed.") \
    \
    M(RegexpCreated, "Compiled regular expressions. Identical regular expressions compiled just once and cached forever.") \
    M(ContextLock, "Number of times the lock of Context was acquired or tried to acquire. This is global lock.") \
    M(ContextLockWaitMicroseconds, "Context lock wait time in microseconds") \
    \
    M(StorageBufferFlush, "") \
    M(StorageBufferErrorOnFlush, "") \
    M(StorageBufferPassedAllMinThresholds, "") \
    M(StorageBufferPassedTimeMaxThreshold, "") \
    M(StorageBufferPassedRowsMaxThreshold, "") \
    M(StorageBufferPassedBytesMaxThreshold, "") \
    M(StorageBufferPassedTimeFlushThreshold, "") \
    M(StorageBufferPassedRowsFlushThreshold, "") \
    M(StorageBufferPassedBytesFlushThreshold, "") \
    M(StorageBufferLayerLockReadersWaitMilliseconds, "Time for waiting for Buffer layer during reading") \
    M(StorageBufferLayerLockWritersWaitMilliseconds, "Time for waiting free Buffer layer to write to (can be used to tune Buffer layers)") \
    \
    M(DictCacheKeysRequested, "") \
    M(DictCacheKeysRequestedMiss, "") \
    M(DictCacheKeysRequestedFound, "") \
    M(DictCacheKeysExpired, "") \
    M(DictCacheKeysNotFound, "") \
    M(DictCacheKeysHit, "") \
    M(DictCacheRequestTimeNs, "") \
    M(DictCacheRequests, "") \
    M(DictCacheLockWriteNs, "") \
    M(DictCacheLockReadNs, "") \
    \
    M(DistributedSyncInsertionTimeoutExceeded, "") \
    M(DataAfterMergeDiffersFromReplica, "") \
    M(DataAfterMutationDiffersFromReplica, "") \
    M(PolygonsAddedToPool, "") \
    M(PolygonsInPoolAllocatedBytes, "") \
    M(RWLockAcquiredReadLocks, "") \
    M(RWLockAcquiredWriteLocks, "") \
    M(RWLockReadersWaitMilliseconds, "") \
    M(RWLockWritersWaitMilliseconds, "") \
    M(DNSError, "Total count of errors in DNS resolution") \
    \
    M(RealTimeMicroseconds, "Total (wall clock) time spent in processing (queries and other tasks) threads (not that this is a sum).") \
    M(UserTimeMicroseconds, "Total time spent in processing (queries and other tasks) threads executing CPU instructions in user space. This include time CPU pipeline was stalled due to cache misses, branch mispredictions, hyper-threading, etc.") \
    M(SystemTimeMicroseconds, "Total time spent in processing (queries and other tasks) threads executing CPU instructions in OS kernel space. This include time CPU pipeline was stalled due to cache misses, branch mispredictions, hyper-threading, etc.") \
    M(MemoryOvercommitWaitTimeMicroseconds, "Total time spent in waiting for memory to be freed in OvercommitTracker.") \
    M(MemoryAllocatorPurge, "Total number of times memory allocator purge was requested") \
    M(MemoryAllocatorPurgeTimeMicroseconds, "Total number of times memory allocator purge was requested") \
    M(SoftPageFaults, "") \
    M(HardPageFaults, "") \
    \
    M(OSIOWaitMicroseconds, "Total time a thread spent waiting for a result of IO operation, from the OS point of view. This is real IO that doesn't include page cache.") \
    M(OSCPUWaitMicroseconds, "Total time a thread was ready for execution but waiting to be scheduled by OS, from the OS point of view.") \
    M(OSCPUVirtualTimeMicroseconds, "CPU time spent seen by OS. Does not include involuntary waits due to virtualization.") \
    M(OSReadBytes, "Number of bytes read from disks or block devices. Doesn't include bytes read from page cache. May include excessive data due to block size, readahead, etc.") \
    M(OSWriteBytes, "Number of bytes written to disks or block devices. Doesn't include bytes that are in page cache dirty pages. May not include data that was written by OS asynchronously.") \
    M(OSReadChars, "Number of bytes read from filesystem, including page cache.") \
    M(OSWriteChars, "Number of bytes written to filesystem, including page cache.") \
    \
    M(PerfCpuCycles, "Total cycles. Be wary of what happens during CPU frequency scaling.")  \
    M(PerfInstructions, "Retired instructions. Be careful, these can be affected by various issues, most notably hardware interrupt counts.") \
    M(PerfCacheReferences, "Cache accesses. Usually this indicates Last Level Cache accesses but this may vary depending on your CPU. This may include prefetches and coherency messages; again this depends on the design of your CPU.") \
    M(PerfCacheMisses, "Cache misses. Usually this indicates Last Level Cache misses; this is intended to be used in con‐junction with the PERFCOUNTHWCACHEREFERENCES event to calculate cache miss rates.") \
    M(PerfBranchInstructions, "Retired branch instructions. Prior to Linux 2.6.35, this used the wrong event on AMD processors.") \
    M(PerfBranchMisses, "Mispredicted branch instructions.") \
    M(PerfBusCycles, "Bus cycles, which can be different from total cycles.") \
    M(PerfStalledCyclesFrontend, "Stalled cycles during issue.") \
    M(PerfStalledCyclesBackend, "Stalled cycles during retirement.") \
    M(PerfRefCpuCycles, "Total cycles; not affected by CPU frequency scaling.") \
    \
    M(PerfCpuClock, "The CPU clock, a high-resolution per-CPU timer") \
    M(PerfTaskClock, "A clock count specific to the task that is running") \
    M(PerfContextSwitches, "Number of context switches") \
    M(PerfCpuMigrations, "Number of times the process has migrated to a new CPU") \
    M(PerfAlignmentFaults, "Number of alignment faults. These happen when unaligned memory accesses happen; the kernel can handle these but it reduces performance. This happens only on some architectures (never on x86).") \
    M(PerfEmulationFaults, "Number of emulation faults. The kernel sometimes traps on unimplemented instructions and emulates them for user space. This can negatively impact performance.") \
    M(PerfMinEnabledTime, "For all events, minimum time that an event was enabled. Used to track event multiplexing influence") \
    M(PerfMinEnabledRunningTime, "Running time for event with minimum enabled time. Used to track the amount of event multiplexing") \
    M(PerfDataTLBReferences, "Data TLB references") \
    M(PerfDataTLBMisses, "Data TLB misses") \
    M(PerfInstructionTLBReferences, "Instruction TLB references") \
    M(PerfInstructionTLBMisses, "Instruction TLB misses") \
    M(PerfLocalMemoryReferences, "Local NUMA node memory reads") \
    M(PerfLocalMemoryMisses, "Local NUMA node memory read misses") \
    \
    M(CreatedHTTPConnections, "Total amount of created HTTP connections (counter increase every time connection is created).") \
    \
    M(CannotWriteToWriteBufferDiscard, "Number of stack traces dropped by query profiler or signal handler because pipe is full or cannot write to pipe.") \
    M(QueryProfilerSignalOverruns, "Number of times we drop processing of a query profiler signal due to overrun plus the number of signals that OS has not delivered due to overrun.") \
    M(QueryProfilerRuns, "Number of times QueryProfiler had been run.") \
    \
    M(CreatedLogEntryForMerge, "Successfully created log entry to merge parts in ReplicatedMergeTree.") \
    M(NotCreatedLogEntryForMerge, "Log entry to merge parts in ReplicatedMergeTree is not created due to concurrent log update by another replica.") \
    M(CreatedLogEntryForMutation, "Successfully created log entry to mutate parts in ReplicatedMergeTree.") \
    M(NotCreatedLogEntryForMutation, "Log entry to mutate parts in ReplicatedMergeTree is not created due to concurrent log update by another replica.") \
    \
    M(S3ReadMicroseconds, "Time of GET and HEAD requests to S3 storage.") \
    M(S3ReadRequestsCount, "Number of GET and HEAD requests to S3 storage.") \
    M(S3ReadRequestsErrors, "Number of non-throttling errors in GET and HEAD requests to S3 storage.") \
    M(S3ReadRequestsThrottling, "Number of 429 and 503 errors in GET and HEAD requests to S3 storage.") \
    M(S3ReadRequestsRedirects, "Number of redirects in GET and HEAD requests to S3 storage.") \
    \
    M(S3WriteMicroseconds, "Time of POST, DELETE, PUT and PATCH requests to S3 storage.") \
    M(S3WriteRequestsCount, "Number of POST, DELETE, PUT and PATCH requests to S3 storage.") \
    M(S3WriteRequestsErrors, "Number of non-throttling errors in POST, DELETE, PUT and PATCH requests to S3 storage.") \
    M(S3WriteRequestsThrottling, "Number of 429 and 503 errors in POST, DELETE, PUT and PATCH requests to S3 storage.") \
    M(S3WriteRequestsRedirects, "Number of redirects in POST, DELETE, PUT and PATCH requests to S3 storage.") \
    \
    M(DiskS3ReadMicroseconds, "Time of GET and HEAD requests to DiskS3 storage.") \
    M(DiskS3ReadRequestsCount, "Number of GET and HEAD requests to DiskS3 storage.") \
    M(DiskS3ReadRequestsErrors, "Number of non-throttling errors in GET and HEAD requests to DiskS3 storage.") \
    M(DiskS3ReadRequestsThrottling, "Number of 429 and 503 errors in GET and HEAD requests to DiskS3 storage.") \
    M(DiskS3ReadRequestsRedirects, "Number of redirects in GET and HEAD requests to DiskS3 storage.") \
    \
    M(DiskS3WriteMicroseconds, "Time of POST, DELETE, PUT and PATCH requests to DiskS3 storage.") \
    M(DiskS3WriteRequestsCount, "Number of POST, DELETE, PUT and PATCH requests to DiskS3 storage.") \
    M(DiskS3WriteRequestsErrors, "Number of non-throttling errors in POST, DELETE, PUT and PATCH requests to DiskS3 storage.") \
    M(DiskS3WriteRequestsThrottling, "Number of 429 and 503 errors in POST, DELETE, PUT and PATCH requests to DiskS3 storage.") \
    M(DiskS3WriteRequestsRedirects, "Number of redirects in POST, DELETE, PUT and PATCH requests to DiskS3 storage.") \
    \
    M(S3DeleteObjects, "Number of S3 API DeleteObject(s) calls.") \
    M(S3CopyObject, "Number of S3 API CopyObject calls.") \
    M(S3ListObjects, "Number of S3 API ListObjects calls.") \
    M(S3HeadObject,  "Number of S3 API HeadObject calls.") \
    M(S3CreateMultipartUpload, "Number of S3 API CreateMultipartUpload calls.") \
    M(S3UploadPartCopy, "Number of S3 API UploadPartCopy calls.") \
    M(S3UploadPart, "Number of S3 API UploadPart calls.") \
    M(S3AbortMultipartUpload, "Number of S3 API AbortMultipartUpload calls.") \
    M(S3CompleteMultipartUpload, "Number of S3 API CompleteMultipartUpload calls.") \
    M(S3PutObject, "Number of S3 API PutObject calls.") \
    M(S3GetObject, "Number of S3 API GetObject calls.") \
    \
    M(DiskS3DeleteObjects, "Number of DiskS3 API DeleteObject(s) calls.") \
    M(DiskS3CopyObject, "Number of DiskS3 API CopyObject calls.") \
    M(DiskS3ListObjects, "Number of DiskS3 API ListObjects calls.") \
    M(DiskS3HeadObject,  "Number of DiskS3 API HeadObject calls.") \
    M(DiskS3CreateMultipartUpload, "Number of DiskS3 API CreateMultipartUpload calls.") \
    M(DiskS3UploadPartCopy, "Number of DiskS3 API UploadPartCopy calls.") \
    M(DiskS3UploadPart, "Number of DiskS3 API UploadPart calls.") \
    M(DiskS3AbortMultipartUpload, "Number of DiskS3 API AbortMultipartUpload calls.") \
    M(DiskS3CompleteMultipartUpload, "Number of DiskS3 API CompleteMultipartUpload calls.") \
    M(DiskS3PutObject, "Number of DiskS3 API PutObject calls.") \
    M(DiskS3GetObject, "Number of DiskS3 API GetObject calls.") \
    \
    M(ReadBufferFromS3Microseconds, "Time spend in reading from S3.") \
    M(ReadBufferFromS3Bytes, "Bytes read from S3.") \
    M(ReadBufferFromS3RequestsErrors, "Number of exceptions while reading from S3.") \
    \
    M(WriteBufferFromS3Bytes, "Bytes written to S3.") \
    \
    M(QueryMemoryLimitExceeded, "Number of times when memory limit exceeded for query.") \
    \
    M(CachedReadBufferReadFromSourceMicroseconds, "Time reading from filesystem cache source (from remote filesystem, etc)") \
    M(CachedReadBufferReadFromCacheMicroseconds, "Time reading from filesystem cache") \
    M(CachedReadBufferReadFromSourceBytes, "Bytes read from filesystem cache source (from remote fs, etc)") \
    M(CachedReadBufferReadFromCacheBytes, "Bytes read from filesystem cache") \
    M(CachedReadBufferCacheWriteBytes, "Bytes written from source (remote fs, etc) to filesystem cache") \
    M(CachedReadBufferCacheWriteMicroseconds, "Time spent writing data into filesystem cache") \
    M(CachedWriteBufferCacheWriteBytes, "Bytes written from source (remote fs, etc) to filesystem cache") \
    M(CachedWriteBufferCacheWriteMicroseconds, "Time spent writing data into filesystem cache") \
    \
    M(RemoteFSSeeks, "Total number of seeks for async buffer") \
    M(RemoteFSPrefetches, "Number of prefetches made with asynchronous reading from remote filesystem") \
    M(RemoteFSCancelledPrefetches, "Number of cancelled prefecthes (because of seek)") \
    M(RemoteFSUnusedPrefetches, "Number of prefetches pending at buffer destruction") \
    M(RemoteFSPrefetchedReads, "Number of reads from prefecthed buffer") \
    M(RemoteFSPrefetchedBytes, "Number of bytes from prefecthed buffer") \
    M(RemoteFSUnprefetchedReads, "Number of reads from unprefetched buffer") \
    M(RemoteFSUnprefetchedBytes, "Number of bytes from unprefetched buffer") \
    M(RemoteFSLazySeeks, "Number of lazy seeks") \
    M(RemoteFSSeeksWithReset, "Number of seeks which lead to a new connection") \
    M(RemoteFSBuffers, "Number of buffers created for asynchronous reading from remote filesystem") \
    \
    M(ThreadpoolReaderTaskMicroseconds, "Time spent getting the data in asynchronous reading") \
    M(ThreadpoolReaderReadBytes, "Bytes read from a threadpool task in asynchronous reading") \
    M(ThreadpoolReaderSubmit, "Bytes read from a threadpool task in asynchronous reading") \
    \
    M(FileSegmentWaitReadBufferMicroseconds, "Metric per file segment. Time spend waiting for internal read buffer (includes cache waiting)") \
    M(FileSegmentReadMicroseconds, "Metric per file segment. Time spend reading from file") \
    M(FileSegmentWriteMicroseconds, "Metric per file segment. Time spend writing cache") \
    M(FileSegmentCacheWriteMicroseconds, "Metric per file segment. Time spend writing data to cache") \
    M(FileSegmentPredownloadMicroseconds, "Metric per file segment. Time spent predownloading data to cache (predownloading - finishing file segment download (after someone who failed to do that) up to the point current thread was requested to do)") \
    M(FileSegmentUsedBytes, "Metric per file segment. How many bytes were actually used from current file segment") \
    \
    M(ReadBufferSeekCancelConnection, "Number of seeks which lead to new connection (s3, http)") \
    \
    M(SleepFunctionCalls, "Number of times a sleep function (sleep, sleepEachRow) has been called.") \
    M(SleepFunctionMicroseconds, "Time spent sleeping due to a sleep function call.") \
    \
    M(ThreadPoolReaderPageCacheHit, "Number of times the read inside ThreadPoolReader was done from page cache.") \
    M(ThreadPoolReaderPageCacheHitBytes, "Number of bytes read inside ThreadPoolReader when it was done from page cache.") \
    M(ThreadPoolReaderPageCacheHitElapsedMicroseconds, "Time spent reading data from page cache in ThreadPoolReader.") \
    M(ThreadPoolReaderPageCacheMiss, "Number of times the read inside ThreadPoolReader was not done from page cache and was hand off to thread pool.") \
    M(ThreadPoolReaderPageCacheMissBytes, "Number of bytes read inside ThreadPoolReader when read was not done from page cache and was hand off to thread pool.") \
    M(ThreadPoolReaderPageCacheMissElapsedMicroseconds, "Time spent reading data inside the asynchronous job in ThreadPoolReader - when read was not done from page cache.") \
    \
    M(AsynchronousReadWaitMicroseconds, "Time spent in waiting for asynchronous reads.") \
    M(AsynchronousRemoteReadWaitMicroseconds, "Time spent in waiting for asynchronous remote reads.") \
    M(SynchronousRemoteReadWaitMicroseconds, "Time spent in waiting for synchronous remote reads.") \
    \
    M(ExternalDataSourceLocalCacheReadBytes, "Bytes read from local cache buffer in RemoteReadBufferCache")\
    \
    M(MainConfigLoads, "Number of times the main configuration was reloaded.") \
    \
    M(AggregationPreallocatedElementsInHashTables, "How many elements were preallocated in hash tables for aggregation.") \
    M(AggregationHashTablesInitializedAsTwoLevel, "How many hash tables were inited as two-level for aggregation.") \
    \
    M(MergeTreeMetadataCacheGet, "Number of rocksdb reads(used for merge tree metadata cache)") \
    M(MergeTreeMetadataCachePut, "Number of rocksdb puts(used for merge tree metadata cache)") \
    M(MergeTreeMetadataCacheDelete, "Number of rocksdb deletes(used for merge tree metadata cache)") \
    M(MergeTreeMetadataCacheSeek, "Number of rocksdb seeks(used for merge tree metadata cache)") \
    M(MergeTreeMetadataCacheHit, "Number of times the read of meta file was done from MergeTree metadata cache") \
    M(MergeTreeMetadataCacheMiss, "Number of times the read of meta file was not done from MergeTree metadata cache") \
    M(ScalarSubqueriesCacheMiss, "Number of times a read from a scalar subquery was not cached and had to be calculated completely")                                                                                                                                                                                                 \
    \
    M(SchemaInferenceCacheHits, "Number of times a schema from cache was used for schema inference") \
    M(SchemaInferenceCacheMisses, "Number of times a schema is not in cache while schema inference") \
    M(SchemaInferenceCacheEvictions, "Number of times a schema from cache was evicted due to overflow") \
    M(SchemaInferenceCacheInvalidations, "Number of times a schema in cache became invalid due to changes in data") \
    \
    M(IOUringSQEsSubmitted, "Total number of io_uring SQEs submitted") \
    M(IOUringSQEsResubmits, "Total number of io_uring SQE resubmits performed") \
    M(IOUringCQEsCompleted, "Total number of successfully completed io_uring CQEs") \
    M(IOUringCQEsFailed, "Total number of completed io_uring CQEs with failures")


namespace ProfileEvents
{

#define M(NAME, DOCUMENTATION) extern const Event NAME = __COUNTER__;
    APPLY_FOR_EVENTS(M)
#undef M
constexpr Event END = __COUNTER__;

/// Global variable, initialized by zeros.
Counter global_counters_array[END] {};
/// Initialize global counters statically
Counters global_counters(global_counters_array);

const Event Counters::num_counters = END;


Counters::Counters(VariableContext level_, Counters * parent_)
    : counters_holder(new Counter[num_counters] {}),
      parent(parent_),
      level(level_)
{
    counters = counters_holder.get();
}

void Counters::resetCounters()
{
    if (counters)
    {
        for (Event i = 0; i < num_counters; ++i)
            counters[i].store(0, std::memory_order_relaxed);
    }
}

void Counters::reset()
{
    parent = nullptr;
    resetCounters();
}

Counters::Snapshot::Snapshot()
    : counters_holder(new Count[num_counters] {})
{}

Counters::Snapshot Counters::getPartiallyAtomicSnapshot() const
{
    Snapshot res;
    for (Event i = 0; i < num_counters; ++i)
        res.counters_holder[i] = counters[i].load(std::memory_order_relaxed);
    return res;
}

const char * getName(Event event)
{
    static const char * strings[] =
    {
    #define M(NAME, DOCUMENTATION) #NAME,
        APPLY_FOR_EVENTS(M)
    #undef M
    };

    return strings[event];
}

const char * getDocumentation(Event event)
{
    static const char * strings[] =
    {
    #define M(NAME, DOCUMENTATION) DOCUMENTATION,
        APPLY_FOR_EVENTS(M)
    #undef M
    };

    return strings[event];
}


Event end() { return END; }


void increment(Event event, Count amount)
{
    DB::CurrentThread::getProfileEvents().increment(event, amount);
}

CountersIncrement::CountersIncrement(Counters::Snapshot const & snapshot)
{
    init();
    std::memcpy(increment_holder.get(), snapshot.counters_holder.get(), Counters::num_counters * sizeof(Increment));
}

CountersIncrement::CountersIncrement(Counters::Snapshot const & after, Counters::Snapshot const & before)
{
    init();
    for (Event i = 0; i < Counters::num_counters; ++i)
        increment_holder[i] = static_cast<Increment>(after[i]) - static_cast<Increment>(before[i]);
}

void CountersIncrement::init()
{
    increment_holder = std::make_unique<Increment[]>(Counters::num_counters);
}

}

#undef APPLY_FOR_EVENTS
