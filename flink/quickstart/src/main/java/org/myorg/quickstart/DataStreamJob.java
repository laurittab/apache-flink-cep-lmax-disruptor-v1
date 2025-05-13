package org.myorg.quickstart;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * A simple stateless Flink DataStream Job.
 * It generates a sequence of numbers, filters even numbers, and prints them.
 */
public class DataStreamJob {

    public static void main(String[] args) throws Exception {
        // Sets up the execution environment for the Flink application
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Create a DataStream (a sequence of numbers in this case)
        DataStream<Long> numbers = env.fromSequence(1, 1000000000);

        // Apply a simple transformation: filter even numbers
        DataStream<Long> filteredNumbers = numbers.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value % 2 == 0; // Only allow even numbers
            }
        });

        // Print the filtered result to stdout
        filteredNumbers.print("Filtered Even Numbers");

        // Execute the job, which starts the data processing
        env.execute("Simple Stateless Flink Job");
    }
}
