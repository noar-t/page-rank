// path for Sunaina = "/Users/sunaina/IdeaProjects/page-rank/src/main/resources/links"
// path for Noah = "/Users/noah/Documents/Projects/IdeaProjects/SparkHello/src/main/resources/links"

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
//import scala.Int;
import scala.Tuple2;

import java.util.ArrayList;


public class Main {

    public static void main(String[] args) {
      // setup runtime

      String path = "/Users/sunaina/IdeaProjects/page-rank/src/main/resources/links/";

      SparkConf conf = new SparkConf().setAppName("test app").setMaster("local");
      JavaSparkContext sc = new JavaSparkContext(conf);
      sc.setLogLevel("ERROR");

      // filename/links pairs
      final JavaPairRDD<String, String> input = sc.wholeTextFiles(path);
      final long numPages = input.count();



      // cleanup file names produce <URL, Contained URLS>
      final JavaPairRDD<String, String> pages = input.mapToPair(x -> {
        String url = x._1;
        url = url.replace("file:" + path, "");
        url = url.replace("%2f", "/");
        return new Tuple2<>(url, x._2);
        });


      // All src/dest url pairs
      final JavaPairRDD<String, String> urlPairs = pages.flatMapToPair(x -> {
        ArrayList<Tuple2<String, String>> urlTuples = new ArrayList<>();
        for (String elem : x._2.split("\n")) {
          //TODO look at other todo
          if (elem.length() > 0)
            urlTuples.add(new Tuple2<>(x._1, elem));
        }
        return urlTuples.listIterator();
      });

      // Get dest -> src pairs
      JavaPairRDD<String, String> reversePairs = urlPairs.mapToPair(x -> x.swap());

      // All possible linked pairs
      JavaPairRDD<String, String> allPairs = urlPairs.union(reversePairs);

      // All possible urls in graph  (src or dest)
      final JavaRDD<String> allLinks = allPairs.map(x -> x._1).distinct();

      // Number of links in the RDD
      final long numLinks = allLinks.count();


      final JavaPairRDD<String, Integer> zeroedWeightedLinks = allLinks.mapToPair(x -> new Tuple2<>(x, 0));
      final JavaPairRDD<String, Integer> weightedOutgoingLinks = urlPairs.mapToPair(x -> new Tuple2<>(x._1, 1));

      // link/#outgoing links for every possible link
      // contains all src from src->dest pairs, and the dest links with no outgoing links, or no files
      final JavaPairRDD<String, Integer> outgoingCount = zeroedWeightedLinks
          .union(weightedOutgoingLinks)
          .reduceByKey((a, b) -> a + b);

      //System.out.println(" --- number of outgoing links --- ");
      //outgoingCount.foreach(x -> System.out.println(x));

      // all the links which have no files or empty files
      final JavaRDD<String> noOutgoingLinks = outgoingCount
          .filter(x -> x._2 == 0)
          .map(x -> x._1);

      // all the links that have non empty files
      final JavaRDD<String> hasOutgoingLinks = outgoingCount
          .filter(x -> x._2 >= 1)
          .map(x -> x._1);

      // TODO add in links contained in allLinks but not yet in outgoingCount



      // ***** This is where the fun starts *****

      //URL to rank mapping
      JavaPairRDD<String, Double> pageRanks = allLinks.mapToPair(x -> new Tuple2<>(x, 1.0));

      for (int i = 0; i < 3; i++) {
        System.out.println("-------------- Iteration:" + i + " -------------");
        pageRanks.foreach(x -> System.out.println(x));

        // Produces (url | cur page rank) of all pages without links
        JavaPairRDD<String, Double> noOutgoingTemp = pageRanks
                .join(outgoingCount)
                .filter(x -> x._2._2 == 0)
                .mapToPair(x -> new Tuple2<>(x._1, x._2._1));

        // Calculate the offset to add to every page from the PRs of the sinks
        Double total = noOutgoingTemp.map(x -> x._2).reduce((a, b) -> a + b);
        Double offset = total / (numLinks - 1);

        // Produces (url | cur page rank) of pages with outgoing links
        JavaPairRDD<String, Tuple2<Double, Integer>> outgoingTempCount = pageRanks
                .join(outgoingCount)
                .filter(x -> x._2._2 >= 1);

        JavaPairRDD<String, Double> outgoingTemp = outgoingTempCount
                .mapToPair(x ->
                        new Tuple2<>(x._1, x._2._1));


        //src link/(PR/#outgoing links)
        JavaPairRDD<String, Double> weightedOutgoingPR = outgoingTempCount
            .mapToPair(x -> {

          System.out.println("Inside the map " + x);
          double effectivePR = x._2._1 / x._2._2;
          return new Tuple2<>(x._1, effectivePR);
        });

        // Calculate how much needs to be subtracted for each page that has no outgoing links
        JavaPairRDD<String, Double> weightedNoOutgoingPR = pageRanks
                .join(noOutgoingTemp)
                .mapToPair(x ->
                    new Tuple2<>(x._1, (-1 * x._2._1) / (numLinks - 1)));

        // Reset the PR of the links with no outgoing links, because the offset gets added back
        //noOutgoingTemp = noOutgoingTemp.mapToPair(x -> new Tuple2<>(x._1, 0.0));

        JavaPairRDD<String, Double> newPRs = outgoingTemp
                .union(noOutgoingTemp)
                .mapToPair(x ->
                        new Tuple2<>(x._1, x._2 + offset));

        //weightedOutgoingPR.foreach(x -> System.out.println(x));

        //TODO also add dampening

        JavaPairRDD<String, Double> destPRs = weightedOutgoingPR
                .join(allPairs)
                .mapToPair(x ->
                        new Tuple2<>(x._2._2, x._2._1));

        destPRs = destPRs.union(weightedNoOutgoingPR);
        destPRs = destPRs.reduceByKey((a, b) -> a + b);

        pageRanks = destPRs;
      }


      System.out.println("-------------- Final Page Ranks -------------");
      pageRanks.foreach(x -> System.out.println(x));
    }
}
