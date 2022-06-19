import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

public class Formatted {
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
        JavaRDD<Row> idealista = spark.read().parquet("./idealista/*").toJavaRDD();
        JavaPairRDD<String, RentInformation> idealista_unique = idealista
                .mapToPair(setKey("propertyCode"))
                .reduceByKey((row1, row2) -> row2).mapValues(RentInformation::new);

        JavaPairRDD<String, String> rent_lookup_neighborhood = readCollectionWithKey(jsc, "rent_lookup_neighborhood", "ne")
                .mapToPair(extract_id);

        JavaPairRDD<String, RentInformation> idealista_rekeyed = idealista_unique
                .mapToPair(row -> new Tuple2<>(row._2().getNeighborhood(), row._2()));

        idealista_rekeyed.take(10).forEach(System.out::println);
        rent_lookup_neighborhood.take(10).forEach(System.out::println);

        JavaPairRDD<String, RentInformation> idealista_joined = idealista_rekeyed
                .join(rent_lookup_neighborhood)
                .mapToPair(pair -> new Tuple2<>(pair._2()._2(), pair._2()._1()));

        System.out.println("idealista: original: " + idealista.count());
        System.out.println("idealista: after duplicate removal: " + idealista_unique.count());
        System.out.println("idealista: after neighborhood join: " + idealista_joined.count());

        // income opendataBCN processing
        JavaPairRDD<String, List<IncomeInfo>> income_opendata_neighborhood = readCollectionWithKey(jsc, "income_opendata_neighborhood", "neigh_name ")
                .mapValues(row -> {
                    List<IncomeInfo> list = new ArrayList<>();
                    row.getList(row.fieldIndex("info"))
                            .forEach(item -> list.add(new IncomeInfo((Row) item)));
                    return list;
                });

        JavaPairRDD<String, String> income_lookup_neighborhood = readCollectionWithKey(jsc,
                "income_lookup_neighborhood", "neighborhood")
                .mapToPair(extract_id);

        JavaPairRDD<String, List<IncomeInfo>> income_opendata_neighborhood_joined = income_opendata_neighborhood
                .join(income_lookup_neighborhood)
                .mapToPair(pair -> new Tuple2<>(pair._2()._2(), pair._2()._1()));

        System.out.println("income_opendata_neighborhood: " + income_opendata_neighborhood.count());
        System.out.println("income_opendata_neighborhood: after neighborhood join: "
                + income_opendata_neighborhood_joined.count());

        JavaPairRDD<String, Integer> incidents = readCollectionWithKey(jsc, "incidents", "Nom barri")
                .filter(
                    tuple -> {
                        Row row = tuple._2();
                        Integer fieldIndex = row.fieldIndex("Codi Incident");
                        return fieldIndex != -1 && !row.isNullAt(fieldIndex) && row.getString(fieldIndex).equals("610");
                    })
                .mapValues(row -> row.isNullAt(9) ? 0 : row.getInt(9)) // Only interested in number of incidents
                .reduceByKey((a, b) -> a + b); // Sum incidents

        JavaPairRDD<String, Integer> incidents_joined = incidents
                .join(income_lookup_neighborhood)
                .mapToPair(pair -> new Tuple2<>(pair._2()._2(), pair._2()._1()));

        System.out.println("incidents: " + incidents.count());
        System.out.println("incidents: after neighborhood join: " + incidents_joined.count());

        JavaPairRDD<String, Tuple2<List<IncomeInfo>, Integer>> join = income_opendata_neighborhood_joined.join(incidents_joined);

        System.out.println("join: " + join.count());

        JavaPairRDD<String, RentInformation> final_join = idealista_joined
            .join(join)
            .mapValues(tuple -> {
                RentInformation rentInformation = tuple._1();
                List<IncomeInfo> incomeInfo = tuple._2()._1();
                Integer inc = tuple._2()._2();
                rentInformation.setIncidents(inc);
                rentInformation.setIncomeInfo(incomeInfo);
                return rentInformation;
            });

        System.out.println("last: " + final_join.count());

        // Print the fist 10 rows
        final_join.take(10).stream().forEach(System.out::println);

        // We do not need the neighborhood id anymore
        JavaRDD<RentInformation> values = final_join.values();

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
