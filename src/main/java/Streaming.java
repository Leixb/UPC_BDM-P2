import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple3;
import scala.Tuple4;

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

        SparkSession spark = SparkSession.builder().appName("P2").getOrCreate();
        SparkContext sc = spark.sparkContext();

        Logger.getRootLogger().setLevel(Level.ERROR);


        LogisticRegressionModel model = LogisticRegressionModel.load(sc, "./model");

        // ssc.checkpoint("./checkpoint");

        JavaInputDStream<ConsumerRecord<String, String>> kafkaStream = KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, createKafkaParamsMap()));

        // The key is not defined in our stream, ignore it
        JavaDStream<String> stream = kafkaStream.map(t -> t.value());
        // stream.print();
        JavaDStream<Tuple3<String, String, Integer>> records = stream.map(record ->{
            String[] l = record.split(",");
            return new Tuple3<String, String, Integer>(l[0], l[1], Integer.parseInt(l[2]));
        });

        // KafkaProducer<String, String> producer = new KafkaProducer<String, String>(createKafkaParamsMap());

        createKafkaParamsMap();

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        JavaDStream<Tuple4<String, String, Integer, Integer> > predicted = records.map(record -> {
            Integer prediction = (int) model.predict(Vectors.dense(new double[] {
                    (double) Model.NeighbourhoodEncode(record._2()),
                    (double) record._3()
            }));
            return new Tuple4<String, String, Integer, Integer>(record._1(), record._2(), record._3(), prediction);
        });

        predicted.foreachRDD(rdd -> {
            rdd.foreach(record -> {
                System.out.println(record._1() + "," + record._2() + "," + record._3() + "," + record._4());

                Producer<String, String> producer = new KafkaProducer<>(props);
                producer.send(new ProducerRecord<String, String>("bdm_p2", record._1() + "," + record._2() + "," + record._3() + "," + record._4()));
                producer.close();
            });
        });

        ssc.start();

        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            System.out.println("INTERRUPTED");
            System.exit(0);
        }
        ssc.stop();
    }
}
