import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.bson.Document;

public class Main {

	public static void main(String[] args) {
		SparkSession spark = SparkSession
            .builder()
            .master("local[*]")
            .appName("P2")
            .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.rent_lookup_district")
            .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.rent_lookup_district")
            .getOrCreate();
		// spark.read().parquet("./idealista/*")
				// .withColumn("input_file", functions.input_file_name())
				// .toJavaRDD()
				// .foreach(t -> System.out.println(t));
        // Read from mongo

        // Create a JavaSparkContext using the SparkSession's SparkContext object
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        /*Start Example: Read data from MongoDB************************/
        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
        /*End Example**************************************************/
        // Analyze data from MongoDB
        System.out.println(rdd.count());
        // System.out.println(rdd.first().toJson());
        jsc.close();

        // spark.read().format("mongodb").option("uri", "mongodb://localhost:27017").load("local.rent_lookup_district").show();

	}
}
