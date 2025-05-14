package org.myorg.quickstart;

import com.lmax.disruptor.*;
import com.lmax.disruptor.support.ValueEvent;
import com.lmax.disruptor.support.ValueAdditionEventHandler;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DataStreamJob {

    public static final int BATCH_SIZE = 10;
    private static final int BUFFER_SIZE = 1024 * 64;
    private static final long ITERATIONS = 1000L * 1000L * 100L;

    private final RingBuffer<ValueEvent> ringBuffer =
        RingBuffer.createSingleProducer(ValueEvent.EVENT_FACTORY, BUFFER_SIZE, new YieldingWaitStrategy());
    private final SequenceBarrier sequenceBarrier = ringBuffer.newBarrier();
    private final ValueAdditionEventHandler handler = new ValueAdditionEventHandler();

    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    public static void main(String[] args) throws Exception {
        // Set up the Flink execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Create a Flink DataStream from Disruptor RingBuffer
        DataStream<ValueEvent> eventStream = env.addSource(new DisruptorSourceFunction());

        // Add transformations or sinks as required
        eventStream
            .keyBy(event -> event.getValue()) // Optional: Adjust based on the use case
            .map(new ValueAdditionEventHandler()) // Apply a custom event handler transformation
            .print(); // Or any other sink like Kafka, file, etc.

        // Start the Flink job
        env.execute("Disruptor to Flink Job");

        // Start the Disruptor producer
        DataStreamJob job = new DataStreamJob();
        job.runDisruptor();
    }

    public void runDisruptor() throws InterruptedException {
        // Initialize Disruptor ring buffer and event processor
        final CountDownLatch latch = new CountDownLatch(1);
        long expectedCount = handler.getSequence() + ITERATIONS * BATCH_SIZE;
        handler.reset(latch, expectedCount);
        executor.submit(handler);
        long start = System.currentTimeMillis();

        for (long i = 0; i < ITERATIONS; i++) {
            long hi = ringBuffer.next(BATCH_SIZE);
            long lo = hi - (BATCH_SIZE - 1);
            for (long l = lo; l <= hi; l++) {
                ringBuffer.get(l).setValue(i);
            }
            ringBuffer.publish(lo, hi);
        }

        latch.await();
        System.out.println("Total Operations: " + (BATCH_SIZE * ITERATIONS));
        long operationsPerSecond = (BATCH_SIZE * ITERATIONS * 1000L) / (System.currentTimeMillis() - start);
        System.out.println("Operations Per Second: " + operationsPerSecond);
        waitForEventProcessorSequence(expectedCount);
    }

    private void waitForEventProcessorSequence(final long expectedCount) throws InterruptedException {
        while (handler.getSequence() != expectedCount) {
            Thread.sleep(1);
        }
    }

    // A custom Flink SourceFunction that pulls events from the Disruptor RingBuffer
    public static class DisruptorSourceFunction implements SourceFunction<ValueEvent> {
        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<ValueEvent> ctx) throws Exception {
            // Pull events from Disruptor's RingBuffer and send to Flink stream
            while (isRunning) {
                long sequence = ringBuffer.next();
                ValueEvent event = ringBuffer.get(sequence);
                ctx.collect(event);
                ringBuffer.publish(sequence);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
