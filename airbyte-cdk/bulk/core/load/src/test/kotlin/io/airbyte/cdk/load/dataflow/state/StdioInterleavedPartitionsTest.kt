/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.load.dataflow.state

import io.airbyte.cdk.load.command.DestinationCatalog
import io.airbyte.cdk.load.dataflow.state.stats.StateStatsEnricher
import io.airbyte.cdk.load.message.DestinationRecordRaw
import io.airbyte.cdk.load.message.StreamCheckpoint
import io.mockk.every
import io.mockk.mockk
import java.util.concurrent.ConcurrentHashMap
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

/**
 * STDIO input as the bulk extract CDK emits it when one stream is split into concurrent partitions
 * (FeedReader.awaitAllPartitionReaders): records of all partitions arrive interleaved, then the
 * stream's states arrive later and in partition order, each with sourceStats.recordCount of its own
 * partition only.
 */
class StdioInterleavedPartitionsTest {

    private val stream = StateKeyClientTest.Fixtures.usersStream
    private lateinit var keyClient: InferredStateKeyClient
    private lateinit var histogramStore: StateHistogramStore
    private lateinit var stateStore: StateStore
    private val flushed = PartitionHistogram(ConcurrentHashMap())

    @BeforeEach
    fun setUp() {
        keyClient = InferredStateKeyClient(DestinationCatalog(listOf(stream)))
        histogramStore = StateHistogramStore()
        val enricher = mockk<StateStatsEnricher>()
        every { enricher.enrich(any(), any()) } answers { firstArg() }
        stateStore = StateStore(keyClient, histogramStore, enricher)
    }

    private fun record() {
        val raw = mockk<DestinationRecordRaw>()
        every { raw.stream } returns stream
        flushed.increment(keyClient.getPartitionKey(raw), 1.0)
    }

    private fun state(count: Long) {
        stateStore.accept(
            StreamCheckpoint(
                stream.unmappedDescriptor.namespace,
                stream.unmappedDescriptor.name,
                """{"partition":"$count"}""",
                count,
            )
        )
    }

    /** Everything that arrived was flushed: drain the store the way PipelineRunner does at the end. */
    private fun flushAllAndDrain(): Int {
        histogramStore.acceptFlushedCounts(flushed)
        var emitted = 0
        while (stateStore.getNextComplete() != null) emitted++
        return emitted
    }

    @Test
    fun `serial partitions flush every state`() {
        repeat(5) { record() }
        state(5)
        state(0)
        repeat(3) { record() }
        state(3)

        assertEquals(3, flushAllAndDrain())
        assertFalse(stateStore.hasStates())
    }

    @Test
    fun `interleaved partitions of one stream still flush every state`() {
        // partition 1: 5 records, partition 2: 0, partition 3: 3 records.
        // Partition 3 finishes first, so its records come out among partition 1's.
        record()
        record()
        repeat(3) { record() } // partition 3
        record()
        record()
        record()
        // FeedReader applies checkpoints in partition order once partition 1 is done.
        state(5)
        state(0)
        state(3)

        // All 8 records that preceded the states were flushed, so all 3 states are safe to emit.
        // On master 14274a6 this fails: window 1 holds 8 records but expects 5, window 3 holds 0
        // but expects 3, and PipelineRunner throws "unflushed states were detected".
        assertEquals(3, flushAllAndDrain())
        assertFalse(stateStore.hasStates())
    }
}
