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

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: Main <action>");
            System.exit(1);
        }
        switch (args[0]) {
            case "formatted":
                Formatted.run(args);
                break;
            case "exploitation":
                Exploitation.run(args);
                break;
            default:
                System.out.println("Unknown action: " + args[0]);
                System.exit(1);
        }
    }
}
