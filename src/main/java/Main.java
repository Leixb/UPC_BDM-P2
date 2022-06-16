import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class Main {

    private static Dataset<Row> readCollection(JavaSparkContext jsc, String collection) {
        Map<String, String> readOverrides = new HashMap<String, String>();
        readOverrides.put("collection", collection);
        ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);

        return MongoSpark.load(jsc, readConfig).toDF();
    }

    private static Dataset<Row> filterBelongs(Dataset<Row> data, final String column_id, final String column_list) {
        final int neighborhood_column_index = data.schema().fieldIndex(column_id);
        final int neighborhood_array_index = data.schema().fieldIndex(column_list);
        return data.filter((x) -> {
            List<String> ls = x.getList(neighborhood_array_index);
            String id = x.getString(neighborhood_column_index);
            return ls != null && id != null && ls.contains(id);
        });
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("P2")
                .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.income_opendata_neighborhood")
                .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.income_opendata_neighborhood")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        // Parquet data sources
        Dataset<Row> idealista = spark.read().parquet("./idealista/*");

        // MongoDB data sources
        Dataset<Row> rent_lookup_district = readCollection(jsc, "rent_lookup_district");
        Dataset<Row> rent_lookup_neighborhood = readCollection(jsc, "rent_lookup_neighborhood");
        Dataset<Row> income_lookup_district = readCollection(jsc, "income_lookup_district");
        Dataset<Row> income_lookup_neighborhood = readCollection(jsc, "income_lookup_neighborhood");
        Dataset<Row> income_opendata_neighborhood = readCollection(jsc, "income_opendata_neighborhood")
                .withColumnRenamed("neigh_name ", "neigh_name"); // Fix space in column name

        Dataset<Row> incidents = readCollection(jsc, "incidents");

        Dataset<Row> incidents_per_barri = incidents
                .filter(incidents.col("Codi Incident").equalTo("610"))
                .groupBy("Nom Barri")
                .count()
                .withColumnRenamed("Nom Barri", "neighborhood")
                .withColumnRenamed("count", "incidents")
                .join(income_lookup_neighborhood.select("_id", "neighborhood"), "neighborhood")
                .drop("neighborhood")
                .withColumnRenamed("_id", "n_id");

        // Join idealista with its lookup tables
        Dataset<Row> joined = idealista
                .join(rent_lookup_neighborhood
                        .withColumnRenamed("_id", "n_id")
                        .select("n_id", "ne"),
                        idealista.col("neighborhood")
                                .equalTo(rent_lookup_neighborhood.col("ne")),
                        "left_outer")
                .withColumnRenamed("neighborhood", "neighborhood_idealista")
                .join(rent_lookup_district.select("_id", "di", "ne_id"),
                        idealista.col("district").equalTo(rent_lookup_district.col("di")),
                        "left_outer")
                .withColumnRenamed("_id", "d_id")
                .withColumnRenamed("district", "district_idealista");

        // Check that all the neighborhoods are in the correct district
        Dataset<Row> idealista_correct = filterBelongs(joined, "n_id", "ne_id").drop("ne_id");
        System.out.println("OK: " + idealista_correct.count() + " / " + joined.count());

        // Join income_opendata_neighborhood with its lookup tables
        Dataset<Row> income_joined = income_opendata_neighborhood
                .withColumnRenamed("district_name", "district")
                .join(income_lookup_district.withColumnRenamed("_id", "d_id").select("d_id", "district",
                        "neighborhood_id"), "district")
                .join(
                        income_lookup_neighborhood.withColumnRenamed("_id", "n_id")
                                .select("n_id", "neighborhood"),
                        functions.col("neigh_name").equalTo(functions.col("neighborhood")),
                        "left_outer");

        // Check that all the neighborhoods are in the correct district
        Dataset<Row> income_correct = filterBelongs(income_joined, "n_id", "neighborhood_id").drop("neighborhood_id");
        System.out.println("OK: " + income_correct.count() + " / " + income_joined.count());

        // Join the datasets
        Dataset<Row> final_join = idealista_correct
                .drop("district_name", "d_id")
                .join(income_joined, "n_id")
                .join(incidents_per_barri, "n_id");

        // Count number of duplicates
        Dataset<Row> duplicates = final_join
                .groupBy("url")
                .count()
                .filter(functions.col("count").gt(1));

        // remove duplicates
        System.out.println("Number of duplicates: " + duplicates.count());
        duplicates.agg(functions.sum("count")).show();

        System.out.println(final_join.count() + " / " + joined.count());

        final_join = final_join.dropDuplicates("url");
        System.out.println("After duplicate removal: " + final_join.count());

        // Save dataset into formatted zone
        String output_dir = "./formatted_zone";
        final_join.write().parquet(output_dir);

        jsc.close();
    }
}
