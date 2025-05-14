package perftest.java.com.lmax.disruptor.sequenced;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.support.ValueEvent;
import com.lmax.disruptor.support.ValueAdditionEventHandler;
import com.lmax.disruptor.util.DaemonThreadFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import com.lmax.disruptor.support.PerfTestUtil;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;



public class OneToOneSequencedBatchThroughputTestFlink
{
    public static final int BATCH_SIZE = 10;
    private static final int BUFFER_SIZE = 1024 * 64;
    private static final long ITERATIONS = 1000L * 1000L * 100L;
    private final ExecutorService executor = Executors.newSingleThreadExecutor(DaemonThreadFactory.INSTANCE);
    private final long expectedResult = PerfTestUtil.accumulatedAddition(ITERATIONS) * BATCH_SIZE;
    private final RingBuffer<ValueEvent> ringBuffer = RingBuffer.createSingleProducer(ValueEvent.EVENT_FACTORY, BUFFER_SIZE, new YieldingWaitStrategy());
    private final SequenceBarrier sequenceBarrier = ringBuffer.newBarrier();
    private final ValueAdditionEventHandler handler = new ValueAdditionEventHandler();
    private final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    public OneToOneSequencedBatchThroughputTestFlink()
    {
        DataStream<ValueEvent> eventStream = env.addSource(new DisruptorSourceFunction(ringBuffer)).map(event ->
        {
            long sequence = event.getValue();
            handler.onEvent(event, sequence, false);
            return event;
        });
        eventStream.keyBy(event -> event.getValue()).map(event ->
        {
            long sequence = ringBuffer.getCursor();
            handler.onEvent(event, sequence, false);
            return event;
        });
    }

    public void startFlinkJob() throws Exception
    {
        env.execute("Disruptor to Flink Job");
    }

    public void runDisruptorPass() throws InterruptedException
    {
        final CountDownLatch latch = new CountDownLatch(1);
        long expectedCount = handler.getCurrentSequence() + ITERATIONS * BATCH_SIZE;
        handler.reset(latch, expectedCount);
        executor.submit(() ->
        {
            try
            {
                long sequence = 0;
                ValueEvent event = new ValueEvent();
                handler.onEvent(event, sequence, false);
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        });
        long start = System.currentTimeMillis();
        for (long i = 0; i < ITERATIONS; i++)
        {
            long hi = ringBuffer.next(BATCH_SIZE);
            long lo = hi - (BATCH_SIZE - 1);
            for (long l = lo; l <= hi; l++)
            {
                ringBuffer.get(l).setValue(i);
            }
            ringBuffer.publish(lo, hi);
        }
        latch.await();
        long operationsPerSecond = (BATCH_SIZE * ITERATIONS * 1000L) / (System.currentTimeMillis() - start);
        System.out.println("Operations Per Second: " + operationsPerSecond);
        waitForEventProcessorSequence(expectedCount);
    }

    private void waitForEventProcessorSequence(final long expectedCount) throws InterruptedException
    {
        while (handler.getCurrentSequence() != expectedCount)
        {
            Thread.sleep(1);
        }
    }

    public static void main(final String[] args) throws Exception
    {
        OneToOneSequencedBatchThroughputTestFlink test = new OneToOneSequencedBatchThroughputTestFlink();
        test.runDisruptorPass();
        test.startFlinkJob();
    }

    public static class DisruptorSourceFunction implements SourceFunction<ValueEvent>
    {
        private final RingBuffer<ValueEvent> ringBuffer;
        private volatile boolean isRunning = true;

        public DisruptorSourceFunction(final RingBuffer<ValueEvent> ringBuffer)
        {
            this.ringBuffer = ringBuffer;
        }

        @Override
        public void run(final SourceContext<ValueEvent> ctx) throws Exception
        {
            while (isRunning)
            {
                long sequence = ringBuffer.next();
                ValueEvent event = ringBuffer.get(sequence);
                ctx.collect(event);
                ringBuffer.publish(sequence);
            }
        }

        @Override
        public void cancel()
        {
            isRunning = false;
        }
    }
}
