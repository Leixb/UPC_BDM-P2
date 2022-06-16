import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.bson.Document;

public class Main {

    private static Dataset<Row> readCollection(JavaSparkContext jsc, String collection) {
        Map<String, String> readOverrides = new HashMap<String, String>();
        readOverrides.put("collection", collection);
        ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);

        return MongoSpark.load(jsc, readConfig).toDF();
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

		Dataset<Row> idealista = spark.read().parquet("./idealista/*");

        Dataset<Row> rent_lookup_district = readCollection(jsc, "rent_lookup_district");
        Dataset<Row> rent_lookup_neighborhood = readCollection(jsc, "rent_lookup_neighborhood");
        // Dataset<Row> income_lookup_district = readCollection(jsc, "income_lookup_district");
        // Dataset<Row> income_lookup_neighborhood = readCollection(jsc, "income_lookup_neighborhood");
        // Dataset<Row> income_opendata_neighborhood = readCollection(jsc, "income_opendata_neighborhood");

        idealista.printSchema();

        rent_lookup_neighborhood.printSchema();
        Dataset<Row> nei = idealista
            .join(rent_lookup_neighborhood.withColumnRenamed("_id", "nei_id"), idealista.col("neighborhood")
            .equalTo(rent_lookup_neighborhood.col("ne")), "left_outer");

        nei.printSchema();

        Dataset<Row> joined = nei
            .join(rent_lookup_district, idealista.col("district")
            .equalTo(rent_lookup_district.col("di")), "left_outer");

        joined.printSchema();

        long nulls = joined.filter(joined.col("_id").isNull().and(joined.col("nei_id").isNull())).count();
        System.out.println("Nulls: " + nulls + " / " + idealista.count());

        Set<String> districtSet = new HashSet<String>();
        idealista
            .select("district")
            .distinct()
            .collectAsList()
            .forEach(row -> districtSet.add(row.getString(0)));

        // save districtSet to  file
        districtSet.forEach(System.out::println);

        idealista.foreach(row -> {
            System.out.println(row.getAs("_id") + " " + row.getAs("neighborhood") + " " + row.getAs("district"));
        });

        // rent_lookup_district.printSchema();
        // long nulls = idealista.join(rent_lookup_district, idealista.col("district").isin(
        //     rent_lookup_district.col("di"),
        //     rent_lookup_district.col("di_n"),
        //     rent_lookup_district.col("di_re")
        // ), "left_outer").filter(functions.col("_id").isNull()).count();


        // Create a JavaSparkContext using the SparkSession's SparkContext object
        // JavaMongoRDD<Document> opendata = MongoSpark.load(jsc);


        // Print the number of documents in the collection
        // System.out.println("Number of documents in opendata: " + opendata.count());
        // System.out.println("Number of documents in rent_lookup_district: " + rent_lookup_district.count());

        jsc.close();
	}
}
