import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class Model {
    public static void run(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("P2")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        SparkContext sc = spark.sparkContext();

        // read from parquet file
        JavaRDD<Row> idealista = spark.read().parquet("./idealista/*").toJavaRDD();

        JavaRDD<LabeledPoint> data = idealista.map(row -> {

            double[] values = new double[2];

            values[0] = row.getDouble(row.fieldIndex("size"));
            values[1] = row.getDouble(row.fieldIndex("price"));

            return new LabeledPoint((double) row.getLong(row.fieldIndex("rooms")), Vectors.dense(values));
        });

        JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[] {0.6, 0.4}, 11L);
        JavaRDD<LabeledPoint> training = splits[0].cache();
        JavaRDD<LabeledPoint> test = splits[1];

        // Run training algorithm to build the model.
        LogisticRegressionModel model = new LogisticRegressionWithLBFGS()
                .setNumClasses(40)
                .run(training.rdd());

        // Compute raw scores on the test set.
        JavaPairRDD<Object, Object> predictionAndLabels = test.mapToPair(p ->
            new Tuple2<>(model.predict(p.features()), p.label()));

        // Get evaluation metrics.
        MulticlassMetrics metrics = new MulticlassMetrics(predictionAndLabels.rdd());
        double accuracy = metrics.accuracy();
        System.out.println("Accuracy = " + accuracy);

        // Save and load model
        model.save(sc, "model");
        // LogisticRegressionModel sameModel = LogisticRegressionModel.load(sc, "model");
    }
}
