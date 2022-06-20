import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

// Custom serializable Classes to parse the input data and encode into the output
import data.RentInformation;
import data.IncomeInfo;
import data.Incident;

public class Formatted {
    // Some helper functions to avoid code duplication
    private static JavaRDD<Row> readCollection(JavaSparkContext jsc, String collection) {
        Map<String, String> readOverrides = new HashMap<String, String>();
        readOverrides.put("collection", collection);
        ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);

        return MongoSpark.load(jsc, readConfig).toDF().toJavaRDD();
    }

    private static JavaPairRDD<String, Row> readCollectionWithKey(JavaSparkContext jsc, final String collection,
            final String key) {
        return readCollection(jsc, collection).mapToPair(setKey(key));
    }

    private static PairFunction<Row, String, Row> setKey(final String key) {
        return row -> new Tuple2<>(row.getString(row.fieldIndex(key)), row);
    }

    public static void run(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("P2")
                .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.income_opendata_neighborhood")
                .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.income_opendata_neighborhood")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        // Helper function to take _id column from the row
        PairFunction<Tuple2<String, Row>, String, String> extract_id = tuple -> {
            Row row = tuple._2();
            return new Tuple2<>(tuple._1(), row.getString(row.fieldIndex("_id")));
        };

        // idealista processing
        //
        // We take idealista and:
        // - remove duplicates (same propertyCode) (favouring the most recent one)
        // - join it with the neighborhood lookup table
        // - set the neighborhood_id as key


        // Note: Since we output debugging information throughout the process,
        // all the RDDs are cached to avoid re-computation. Most of them could
        // be removed if we also remove all the debug statements.

        JavaRDD<Row> idealista = spark.read().parquet("./idealista/*").toJavaRDD();
        JavaPairRDD<String, RentInformation> idealista_unique = idealista
                .mapToPair(setKey("propertyCode"))
                .reduceByKey((row1, row2) -> row2)
                .mapValues(RentInformation::new)
                .cache();

        JavaPairRDD<String, String> rent_lookup_neighborhood = readCollectionWithKey(jsc, "rent_lookup_neighborhood", "ne")
                .mapToPair(extract_id)
                .cache();

        JavaPairRDD<String, RentInformation> idealista_rekeyed = idealista_unique
                .mapToPair(row -> new Tuple2<>(row._2().getNeighborhood(), row._2()))
                .cache();

        idealista_rekeyed.take(10).forEach(System.out::println);
        rent_lookup_neighborhood.take(10).forEach(System.out::println);

        JavaPairRDD<String, RentInformation> idealista_joined = idealista_rekeyed
                .join(rent_lookup_neighborhood)
                .mapToPair(pair -> new Tuple2<>(pair._2()._2(), pair._2()._1()))
                .cache();

        System.out.println("idealista: original: " + idealista.count());
        System.out.println("idealista: after duplicate removal: " + idealista_unique.count());
        System.out.println("idealista: after neighborhood join: " + idealista_joined.count());

        // income opendataBCN processing
        JavaPairRDD<String, IncomeInfo> income_opendata_neighborhood = readCollectionWithKey(jsc, "income_opendata_neighborhood", "neigh_name ")
                .flatMapValues(row -> row.getList(row.fieldIndex("info")).iterator()) // Flatten incomeInfo list
                .mapValues(v -> new Tuple2<>(new IncomeInfo((Row) v), 1)) // convert to tuple to compute average per neighborhood
                .reduceByKey((row1, row2) -> new Tuple2<>(row1._1().add(row2._1()), row1._2() + row2._2()))
                .mapValues(tuple -> tuple._1().divide(tuple._2()))
                .cache();

        JavaPairRDD<String, String> income_lookup_neighborhood = readCollectionWithKey(jsc, "income_lookup_neighborhood", "neighborhood")
                .mapToPair(extract_id)
                .cache(); // This is the only .cache() needed even if we remove debug statements

        JavaPairRDD<String, IncomeInfo> income_opendata_neighborhood_joined = income_opendata_neighborhood
                .join(income_lookup_neighborhood)
                .mapToPair(pair -> new Tuple2<>(pair._2()._2(), pair._2()._1()))
                .cache();

        System.out.println("income_opendata_neighborhood: " + income_opendata_neighborhood.count());
        System.out.println("income_opendata_neighborhood: after neighborhood join: " + income_opendata_neighborhood_joined.count());

        // The incidents dataset has two different formats, one with underscores
        // and one without. Thus we need to handle both:

        JavaPairRDD<String, Double> incidents = readCollection(jsc, "incidents")
                // First we remove all the rows that are not our target incident type (610):
                .filter(row -> Incident.codeEquals(row, "610"))
                .map(Incident::new)
                // Now we set Nom barri and year as the key
                .mapToPair(incident -> new Tuple2<>(new Tuple2<String, Integer>(incident.getNeighborhood() , incident.getYear()), incident.getCount()))
                .reduceByKey((a, b) -> a + b) // Reduce by adding all incidents (by neighborhood and year)
                // Change key to neighborhood and add count so that we can compute average:
                .mapToPair(pair -> new Tuple2<>(pair._1()._1(), new Tuple2<Integer, Integer>(pair._2(), 1)))
                .reduceByKey((a, b) -> new Tuple2<>(a._1() + b._1(), a._2() + b._2()))
                .mapValues(pair -> ((double) pair._1()) / pair._2()) // Calculate the average
                .cache();

        // Simple check to see the number of incidents has been reduced properly
        Double total_incidents = incidents.values().reduce((a, b) -> a + b);
        System.out.println("Total incidients: " + total_incidents);
        if (total_incidents == 0)
            throw new RuntimeException("Total incidents is zero");

        JavaPairRDD<String, Double> incidents_joined = incidents
                .join(income_lookup_neighborhood)
                .mapToPair(pair -> new Tuple2<>(pair._2()._2(), pair._2()._1()))
                .cache();

        System.out.println("incidents: " + incidents.count());
        System.out.println("incidents: after neighborhood join: " + incidents_joined.count());

        JavaPairRDD<String, Tuple2<IncomeInfo, Double>> join = income_opendata_neighborhood_joined
                .join(incidents_joined)
                .cache();

        System.out.println("join: " + join.count());

        JavaPairRDD<String, RentInformation> final_join = idealista_joined
                .join(join)
                .mapValues(tuple -> {
                    final RentInformation rentInformation = tuple._1();
                    final IncomeInfo incomeInfo = tuple._2()._1();
                    final Double avg_incidents = tuple._2()._2();
                    rentInformation.setIncidents(avg_incidents);
                    rentInformation.setIncomeInfo(incomeInfo);
                    return rentInformation;
                })
                .cache();

        System.out.println("last: " + final_join.count());

        // Print the fist 10 rows
        final_join.take(10).stream().forEach(System.out::println);

        // Add the neighborhood_id to rentInformation and remove it from key
        JavaRDD<RentInformation> values = final_join.map(tuple -> {
            RentInformation r = tuple._2();
            r.setNeighborhood_id(tuple._1());
            return r;
        }).cache();

        // Convert to a DataFrame using the Class Schema
        Dataset<Row> d = spark.createDataset(JavaRDD.toRDD(values), Encoders.bean(RentInformation.class)).toDF();

        // Show the final schema
        d.printSchema();

        // Check that incidents are properly set
        System.out.println("Incidients: " + values.first().getIncidents());

        // Write to parquet and overwrite
        d.write().mode(SaveMode.Overwrite).parquet("./formatted_zone");

        jsc.close();
    }
}
