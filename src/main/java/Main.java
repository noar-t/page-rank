import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import scala.Int;
import scala.Tuple2;

import java.util.ArrayList;


public class Main {

    public static void main(String[] args) {
      // setup runtime
      SparkConf conf = new SparkConf().setAppName("test app").setMaster("local");
      JavaSparkContext sc = new JavaSparkContext(conf);
      sc.setLogLevel("ERROR");

      // filename/links pairs
      final JavaPairRDD<String, String> input = sc.wholeTextFiles("/Users/noah/Documents/Projects/IdeaProjects/SparkHello/src/main/resources/links");
      final long numPages = input.count();



      // cleanup file names produce <URL, Contained URLS>
      final JavaPairRDD<String, String> pages = input.mapToPair(x -> {
        String url = x._1;
        url = url.replace("file:/Users/noah/Documents/Projects/IdeaProjects/SparkHello/src/main/resources/links/", "");
        url = url.replace("%2f", "/");
        return new Tuple2<>(url, x._2);
        });


      // All src/dest url pairs
      final JavaPairRDD<String, String> urlPairs = pages.flatMapToPair(x -> {
        ArrayList<Tuple2<String, String>> urlTuples = new ArrayList<Tuple2<String, String>>();
        for (String elem : x._2.split("\n")) {
          //TODO look at other todo
          if (elem.length() > 0)
            urlTuples.add(new Tuple2<String, String>(x._1, elem));
        }
        return urlTuples.listIterator();
      });

      // Get dest -> src pairs
      JavaPairRDD<String, String> reversePairs = urlPairs.mapToPair(x -> x.swap());
      JavaPairRDD<String, String> allPairs = urlPairs.union(reversePairs);
      // All possible urls in graph  (src or dest)
      final JavaRDD<String> allLinks = allPairs.map(x -> x._1).distinct();

      // 0 indexed
      final long numLinks = allLinks.count() - 1;

      final JavaPairRDD<String, Integer> zeroedWeightedLinks = allLinks.mapToPair(x -> new Tuple2<>(x, 0));
      final JavaPairRDD<String, Integer> weightedOutgoingLinks = urlPairs.mapToPair(x -> new Tuple2<>(x._1, 1));

      // link/#outgoing links
      final JavaPairRDD<String, Integer> outgoingCount = zeroedWeightedLinks
          .union(weightedOutgoingLinks)
          .reduceByKey((a, b) -> a + b);

      final JavaRDD<String> noOutgoingLinks = outgoingCount
          .filter(x -> x._2 == 0)
          .map(x -> x._1);

      final JavaRDD<String> hasOutgoingLinks = outgoingCount
          .filter(x -> x._2 == 1)
          .map(x -> x._1);

      // TODO add in links contained in allLinks but not yet in outgoingCount


      //URL to rank maping
      JavaPairRDD<String, Double> pageRanks = allLinks.mapToPair(x -> new Tuple2<>(x, new Double(1)));

      for (int i = 0; i < 1; i++) {
        System.out.println("-------------- Iteration:" + i + " -------------");
        pageRanks.foreach(x -> System.out.println(x));

        //TODO need to do outerjoin in order to get sites that are sinks and deal with them


        //src link/(PR/#outgoing links)
        JavaPairRDD<String, Double> weightedOutgoingPR = pageRanks
            .join(outgoingCount)
            .mapToPair(x -> {
          double denominator = x._2._2;
          if (denominator == 0)
            denominator = numLinks;
          double effectivePR = x._2._1 / denominator;
          return new Tuple2<>(x._1, effectivePR);
        });
        System.out.println("---------------effective prs------------");

        weightedOutgoingPR.foreach(x -> System.out.println(x));


        JavaRDD<Double> tempNoOutPR = weightedOutgoingPR
            .join(noOutgoingLinks
            .mapToPair(x -> new Tuple2<>(x, 0.0)))
            .map(x -> x._2._1);

        //TODO add the offset to all pages, since all pr without outgoing links need to be redistributed
        //TODO subtract prs from each page without links such that adding the offset will work
        //TODO also add dampening

        tempNoOutPR.foreach(x -> System.out.println(x));

        //JavaPairRDD<String, Tuple2<Double, Integer>> tempOutPR = effectiveSrcPR.join(weightedOutgoingLinks);

        //Double offset = tempNoOutPR.reduce((a, b) -> a + b);


        //dest link/incoming pr delta
        JavaPairRDD<String, Double> destNewPR = weightedOutgoingPR.join(urlPairs).mapToPair(x -> new Tuple2<>(x._2._2, x._2._1));

        System.out.println("---------------dest new pr------------");

        destNewPR.foreach(x -> System.out.println(x));

        //dest link/new pr
        pageRanks = destNewPR.reduceByKey((a, b) -> a + b);
        System.out.println("---------------page ranks------------");

        pageRanks.foreach(x -> System.out.println(x));
        System.out.println("---------------End of iteration------------");
      }

    }
}
