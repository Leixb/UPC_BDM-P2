import java.util.Map;

import com.google.common.collect.Iterators;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;

public class Streaming {

    private static Collection<String> topics = Arrays.asList("bdm_p2");

    private static Map<String, Object> createKafkaParamsMap() {
        Map<String, Object> myMap = new HashMap<String, Object>();
        myMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "venomoth.fib.upc.edu:9092");
        myMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringDeserializer.class);
        myMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringDeserializer.class);
        myMap.put(ConsumerConfig.GROUP_ID_CONFIG, "gid");
        return myMap;
    }

    public static void run(String[] args) {
        SparkConf conf = new SparkConf().setAppName("P2").setMaster("local[*]");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(1000));
        Logger.getRootLogger().setLevel(Level.ERROR);

        ssc.checkpoint("./checkpoint");

        JavaInputDStream<ConsumerRecord<String, String>> kafkaStream = KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, createKafkaParamsMap()));

        // The key is not defined in our stream, ignore it
        JavaDStream<String> stream = kafkaStream.map(t -> t.value());
        // stream.print();
        JavaDStream<String> records = stream.flatMap(record -> Iterators.forArray(record.split(",")));
        JavaDStream<String> neighborhoods = records.filter(neighborhood -> neighborhood.startsWith("Q"));
        neighborhoods.print();

        // Count of each neighborhood
        JavaPairDStream<String, Integer> neighborhoodsWith1 = neighborhoods
                .mapToPair(neighborhood -> new Tuple2<>(neighborhood, 1));

        JavaPairDStream<String, Integer> counts = neighborhoodsWith1.reduceByKeyAndWindow(
                (i1, i2) -> i1 + i2,
                (i1, i2) -> i1 - i2,
                new Duration(60 * 5 * 1000),
                new Duration(1 * 1000));

        JavaPairDStream<Integer, String> swappedCounts = counts.mapToPair(count -> count.swap());
        JavaPairDStream<Integer, String> sortedCounts = swappedCounts.transformToPair(count -> count.sortByKey(false));

        sortedCounts.foreachRDD(rdd -> {
            String out = "\nCountedNeighborhood:\n";
            for (Tuple2<Integer, String> t : rdd.take(20)) {
                out = out + t.toString() + "\n";
            }
            System.out.println(out);
        });
        ssc.start();

        try {
            ssc.awaitTermination();
        } catch (Exception e) {
            System.exit(0);
        }
        ssc.stop();
    }
}
