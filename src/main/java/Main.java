import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.SparkConf;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;


public class Main {

    public static void main(String[] args) {
      // setup runtime
      SparkConf conf = new SparkConf().setAppName("test app").setMaster("local");
      JavaSparkContext sc = new JavaSparkContext(conf);
      sc.setLogLevel("ERROR");

      // filename/links pairs
      final JavaPairRDD<String, String> input = sc.wholeTextFiles("file:///tmp/links");
      final long numPages = input.count();



      // cleanup file names
      final JavaPairRDD<String, String> pages = input.mapToPair(x -> {
        String url = x._1;
        url = url.replace("file:/tmp/links/", "");
        url = url.replace("%2f", "/");
        return new Tuple2<>(url, x._2);
        });

      //URL to rank maping
      JavaPairRDD<String, Double> pageRanks = pages.mapToPair(x -> new Tuple2<>(x._1, new Double(1)));

      for (int i = 0; i < 10; i++) {
        System.out.println("-------------- Iteration:" + i + " -------------");
        // All src/dest url pairs
        JavaPairRDD<String, String> urlPairs = pages.flatMapToPair(x -> {
          ArrayList<Tuple2<String, String>> urlTuples = new ArrayList<Tuple2<String, String>>();
          for (String elem : x._2.split("\n")) {
            //TODO look at other todo
            if (elem.length() > 0)
              urlTuples.add(new Tuple2<String, String>(x._1, elem));
          }
          return urlTuples.listIterator();
        });


        // link/#outgoing links
        final JavaPairRDD<String, Integer> outgoingCount = urlPairs.mapToPair(x -> new Tuple2<>(x._1, 1)).reduceByKey((a, b) -> a + b);

        //TODO need to do outerjoin in order to get sites that are sinks and deal with them

        //src link/(PR/#outgoing links)
        JavaPairRDD<String, Double> effectiveSrcPR = pageRanks.join(outgoingCount).mapToPair(x -> {
          double effectivePR = x._2._1 / x._2._2;
          return new Tuple2<>(x._1, effectivePR);
        });

        //dest link/incoming pr delta
        JavaPairRDD<String, Double> destNewPR = effectiveSrcPR.join(urlPairs).mapToPair(x -> new Tuple2<>(x._2._2, x._2._1));

        //dest link/new pr
        pageRanks = destNewPR.reduceByKey((a, b) -> a + b);
        pageRanks.foreach(x -> System.out.printf("%.20s   | PR %2.3f\n", x._1, x._2));
      }

    }
}
