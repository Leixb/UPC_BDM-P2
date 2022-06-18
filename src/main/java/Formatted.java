import java.util.HashMap;
import java.util.Map;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;
import scala.Tuple3;

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

    private static PairFunction<Tuple2<String, Row>, String, Row> newKey(final String key) {
        return tuple -> {
            Row row = tuple._2();
            return new Tuple2<String, Row>(row.getString(row.fieldIndex(key)), row);
        };
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

        //
        // Helper functions
        //
        // --------------------------------------------------------------------------------------------------------------------

        // Take only the _id column from the row
        PairFunction<Tuple2<String, Row>, String, String> extract_id = tuple -> {
            Row row = tuple._2();
            return new Tuple2<>(tuple._1(), row.getString(row.fieldIndex("_id")));
        };

        // Use the newly joined lookup value as key
        // PairFunction<Tuple2<String, Tuple2<Row, String>>, String, Row> reset_key =
        // tuple -> new Tuple2<>(tuple._2()._2(), tuple._2()._1());
        PairFunction<Tuple2<String, Tuple2<Row, String>>, String, Row> reset_key = tuple -> new Tuple2<>(
                tuple._2()._2(), tuple._2()._1());

        // --------------------------------------------------------------------------------------------------------------------

        // idealista processing
        //
        // We take idealista and:
        // - remove duplicates (same propertyCode) (favouring the most recent one)
        // - join it with the neighborhood lookup table
        // - set the neighborhood_id as key
        JavaRDD<Row> idealista = spark.read().parquet("./idealista/*").toJavaRDD();
        JavaPairRDD<String, Row> idealista_unique = idealista
                .mapToPair(setKey("propertyCode"))
                .reduceByKey((row1, row2) -> row2);

        JavaPairRDD<String, String> rent_lookup_neighborhood = readCollectionWithKey(jsc, "rent_lookup_neighborhood",
                "ne")
                .mapToPair(extract_id);

        JavaPairRDD<String, Row> idealista_joined = idealista_unique
                .mapToPair(newKey("neighborhood"))
                .join(rent_lookup_neighborhood)
                .mapToPair(reset_key);

        System.out.println("idealista: original: " + idealista.count());
        System.out.println("idealista: after duplicate removal: " + idealista_unique.count());
        System.out.println("idealista: after neighborhood join: " + idealista_joined.count());

        // income opendataBCN processing
        JavaPairRDD<String, Row> income_opendata_neighborhood = readCollectionWithKey(jsc,
                "income_opendata_neighborhood", "neigh_name ");
        JavaPairRDD<String, String> income_lookup_neighborhood = readCollectionWithKey(jsc,
                "income_lookup_neighborhood", "neighborhood")
                .mapToPair(extract_id);

        JavaPairRDD<String, Row> income_opendata_neighborhood_joined = income_opendata_neighborhood
                .join(income_lookup_neighborhood)
                .mapToPair(reset_key);

        System.out.println("income_opendata_neighborhood: " + income_opendata_neighborhood.count());
        System.out.println("income_opendata_neighborhood: after neighborhood join: "
                + income_opendata_neighborhood_joined.count());

        JavaPairRDD<String, Row> incidents = readCollectionWithKey(jsc, "incidents", "Nom barri").filter(
                tuple -> {
                    Row row = tuple._2();
                    Integer fieldIndex = row.fieldIndex("Codi Incident");
                    return fieldIndex != -1 && !row.isNullAt(fieldIndex) && row.getString(fieldIndex).equals("610");
                });

        JavaPairRDD<String, Row> incidents_joined = incidents
                .reduceByKey((row1, row2) -> {
                    int sum = 0;
                    if (row1.schema() != null) {
                        int idx1 = row1.fieldIndex("Numero_incidents_GUB");
                        if (!row1.isNullAt(idx1))
                            sum += row1.getInt(idx1);
                    }
                    if (row2.schema() != null) {
                        int idx2 = row2.fieldIndex("Numero_incidents_GUB");
                        if (!row2.isNullAt(idx2))
                            sum += row2.getInt(idx2);
                    }
                    return RowFactory.create(sum);
                })
                .join(income_lookup_neighborhood)
                .mapToPair(reset_key);

        System.out.println("incidents: " + incidents.count());
        System.out.println("incidents: after neighborhood join: " + incidents_joined.count());

        JavaPairRDD<String, Tuple2<Row, Row>> join = income_opendata_neighborhood_joined.join(incidents_joined);

        System.out.println("join: " + join.count());

        JavaPairRDD<String, Tuple3<Row, Row, Row>> last = idealista_joined.join(join).mapValues(
                tuple -> {
                    Row row1 = tuple._1();
                    Row row2 = tuple._2()._1();
                    Row row3 = tuple._2()._2();
                    return new Tuple3<>(row1, row2, row3);
                });

        System.out.println("last: " + last.count());

        last.saveAsTextFile("formatted_zone");

        jsc.close();
    }
}
