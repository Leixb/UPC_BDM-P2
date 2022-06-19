import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
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
    public static int NeighbourhoodEncode(String s) {
        return Arrays.asList(new String[] {
                "Q1026658", "Q1167101", "Q1404773", "Q1425291", "Q15225338",
                "Q1627690", "Q17154",   "Q1758503", "Q1904302", "Q1932090",
                "Q2057331", "Q2107762", "Q2390761", "Q2442135", "Q2470217",
                "Q2476184", "Q2562684", "Q2736444", "Q2736697", "Q2741611",
                "Q2979686", "Q2994821", "Q3043699", "Q3045547", "Q3291762",
                "Q3294440", "Q3294602", "Q3296693", "Q3297056", "Q3297875",
                "Q3297889", "Q3297896", "Q3298492", "Q3298502", "Q3298510",
                "Q3310216", "Q3319496", "Q3319498", "Q3320699", "Q3320705",
                "Q3320716", "Q3320722", "Q3320806", "Q3320857", "Q3321657",
                "Q3321797", "Q3321805", "Q3389521", "Q3596095", "Q3596096",
                "Q3750551", "Q3750558", "Q3750859", "Q3750926", "Q3750929",
                "Q3750932", "Q3750940", "Q3751072", "Q3751076", "Q3751386",
                "Q3753110", "Q377070",  "Q3773169", "Q3773462", "Q3813816",
                "Q3813818", "Q524311",  "Q542473",  "Q6273541", "Q6746220",
                "Q720994",  "Q980253",  "Q990510",
        }).indexOf(s);
    }

    public static void run(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("P2")
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        SparkContext sc = spark.sparkContext();

        // read from parquet file
        JavaRDD<Row> source_data = spark.read().parquet("./formatted_zone/*").toJavaRDD();

        JavaRDD<LabeledPoint> data = source_data.map(row -> {

            double[] values = new double[] {
                    (double) NeighbourhoodEncode(row.getString(row.fieldIndex("neighborhood_id"))),
                    row.getDouble(row.fieldIndex("price")),
            };


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
        double recall = metrics.weightedRecall();
        System.out.println("Recall = " + recall);
        System.out.println("Accuracy = " + accuracy);

        try {
            FileUtils.deleteDirectory(new File("./model"));
        } catch (IOException e) {
            // no model folder
        }

        // Save and load model
        model.save(sc, "./model");
    }
}
