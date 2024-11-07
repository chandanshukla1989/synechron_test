package org.example;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Properties;

import org.apache.flink.core.fs.FileSystem;

public class KafkaProcessor {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flink-consumer-group");

        // Kafka Consumer
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("INPUT_TOPIC", new SimpleStringSchema(), properties);
        DataStream<String> input = env.addSource(consumer);

        // Process data with added logging and validation
        DataStream<String> processed = input.map(line -> {
            try {
                System.out.println("Received input line: " + line);
                String[] parts = line.split(",");
                if (parts.length != 3) {
                    System.err.println("Invalid input format: " + line);
                    return "INVALID: " + line;
                }

                String name = parts[0].trim();
                String address = parts[1].trim();
                LocalDate dob;
                try {
                    dob = LocalDate.parse(parts[2].trim(), DateTimeFormatter.ISO_LOCAL_DATE);
                } catch (DateTimeParseException e) {
                    System.err.println("Invalid date format for input: " + line);
                    return "INVALID_DATE: " + line;
                }

                Person person = new Person(name, address, dob);
                int age = person.getAge();
                System.out.println("Processed person: " + person + ", Age: " + age);

                // Categorize based on age
                if (age % 2 == 0) {
                    return "EVEN:" + person.toString();
                } else {
                    return "ODD:" + person.toString();
                }
            } catch (Exception e) {
                System.err.println("Error processing line: " + line);
                e.printStackTrace();
                return "ERROR: " + line;
            }
        });

        // Separate into even and odd streams
        DataStream<String> evenStream = processed.filter(value -> value.startsWith("EVEN:"));
        DataStream<String> oddStream = processed.filter(value -> value.startsWith("ODD:"));

        // Kafka Producers
        FlinkKafkaProducer<String> evenProducer = new FlinkKafkaProducer<>(
                "EVEN_TOPIC", new SimpleStringSchema(), properties);
        FlinkKafkaProducer<String> oddProducer = new FlinkKafkaProducer<>(
                "ODD_TOPIC", new SimpleStringSchema(), properties);

        // Add sinks with logging
        evenStream.map(value -> {
            String result = value.substring(5); // Remove "EVEN:" prefix
            System.out.println("Sending to EVEN_TOPIC: " + result);
            return result;
        }).addSink(evenProducer);

        oddStream.map(value -> {
            String result = value.substring(4); // Remove "ODD:" prefix
            System.out.println("Sending to ODD_TOPIC: " + result);
            return result;
        }).addSink(oddProducer);

        // Optional: Persist messages to a file with logging
        processed.map(value -> {
            System.out.println("Writing to file: " + value);
            return value;
        }).writeAsText("/home/azureuser/flink/output.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);


        // Execute the Flink job
        env.execute("Kafka Processor");
    }
}
