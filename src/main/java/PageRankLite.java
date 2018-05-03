// path for Sunaina = "/Users/sunaina/IdeaProjects/page-rank/src/main/resources/links"
// path for Noah = "/Users/noah/Documents/Projects/IdeaProjects/SparkHello/src/main/resources/links"

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.ArrayList;


public class PageRankLite {

    public static void main(String[] args) {
        // setup runtime

        String path = "/Users/sunaina/IdeaProjects/page-rank/src/main/resources/test_links";

        SparkConf conf = new SparkConf().setAppName("test app").setMaster("local[1]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        long startTime = System.nanoTime();
        // filename/links pairs
        final JavaPairRDD<String, String> input = sc.wholeTextFiles(path);


        // cleanup file names produce <URL, Contained URLS>
        final JavaPairRDD<String, String> pages = input.mapToPair(x -> {
            String url = x._1;
            url = url.replace("file:" + path + "/", "");
            url = url.replace("%2f", "/");
            return new Tuple2<>(url, x._2);
        });


        // All src/dest url pairs
        final JavaPairRDD<String, String> urlPairs = pages.flatMapToPair(x -> {
            ArrayList<Tuple2<String, String>> urlTuples = new ArrayList<>();
            for (String elem : x._2.split("\n")) {
                if (elem.length() > 0)
                    urlTuples.add(new Tuple2<>(x._1, elem));
            }
            return urlTuples.listIterator();
        });

        // Number of links in the RDD
        final long numLinks = urlPairs.count();
        final Double dampingFactor = 0.85;

        //final JavaPairRDD<String, Integer> zeroedWeightedLinks = allLinks.mapToPair(x -> new Tuple2<>(x, 0));
        final JavaPairRDD<String, Integer> weightedOutgoingLinks = urlPairs.mapToPair(x -> new Tuple2<>(x._1, 1));

        // link/#outgoing links for every possible link
        final JavaPairRDD<String, Integer> outgoingCount = weightedOutgoingLinks
                .reduceByKey((a, b) -> a + b);
        outgoingCount.persist(StorageLevel.MEMORY_ONLY());

        long setupTime = System.nanoTime() - startTime;
        System.out.println("Setup time = " + setupTime + " nanoseconds");


        // ***** This is where the fun starts *****

        startTime = System.nanoTime();

        //URL to rank mapping
        JavaPairRDD<String, Double> pageRanks = urlPairs.mapToPair(x -> new Tuple2<>(x._1, 1.0));
        pageRanks.persist(StorageLevel.MEMORY_ONLY());

        for (int i = 0; i < 50; i++) {
            //System.out.println("-------------- Iteration:" + i + " -------------");
            //pageRanks.foreach(x -> System.out.println(x));

            JavaPairRDD<String, Tuple2<Double, Integer>> prsAndOutgoingCount = pageRanks
                    .join(outgoingCount);

            // Produces (url | cur page rank) of pages with outgoing links
            JavaPairRDD<String, Tuple2<Double, Integer>> outgoingTempCount = prsAndOutgoingCount
                    .filter(x -> x._2._2 >= 1);

            //src link/(PR/#outgoing links)
            JavaPairRDD<String, Double> weightedOutgoingPR = outgoingTempCount
                    .mapToPair(x -> {
                        double effectivePR = x._2._1 / x._2._2;
                        return new Tuple2<>(x._1, effectivePR);
                    });

            JavaPairRDD<String, Double> destPRs = weightedOutgoingPR
                    .join(urlPairs)
                    .mapToPair(x ->
                            new Tuple2<>(x._2._2, x._2._1));

            destPRs = destPRs.reduceByKey((a, b) -> a + b);

            // Added Damping Factor
            destPRs = destPRs.mapToPair(x -> new Tuple2<>(x._1, 1 - dampingFactor + dampingFactor * x._2));


            pageRanks = destPRs;
        }

        long pageRankTime = System.nanoTime() - startTime;

        System.out.println("PageRank algorithm time = " + pageRankTime + " nanoseconds");

        startTime = System.nanoTime();
        System.out.println("-------------- Final Page Ranks -------------");
        pageRanks.foreach(x -> System.out.println(x));

        long printTime = System.nanoTime() - startTime;
        System.out.println("Print time = " + printTime + " nanoseconds");

    }
}
