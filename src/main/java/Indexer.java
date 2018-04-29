import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.SparkConf;
import scala.Tuple2;

import java.util.*;


public class Indexer {

    public static void main(String[] args) {

        // setup runtime
        SparkConf conf = new SparkConf().setAppName("test app").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");


        Scanner key = new Scanner(System.in);

        System.out.print("Enter upto five words you want to search for: ");
        String line = key.nextLine();
        HashSet<String> keywordsStr= new HashSet<>(Arrays.asList(line.split(" ")));


        //String[] keywordsStr = {"computer" , "education"};


        // filename/links pairs
        final JavaPairRDD<String, String> input = sc.wholeTextFiles("/Users/sunaina/IdeaProjects/page-rank/src/main/resources/tmp/body");


        // cleanup file names
        final JavaPairRDD<String, String> pages = input.mapToPair(x -> {
            String url = x._1;
            url = url.replace("file:/Users/sunaina/IdeaProjects/page-rank/src/main/resources/tmp/body/", "");
            url = url.replace("%2f", "/");
            return new Tuple2<>(url, x._2);
        });

        // url to keywords (removing , and .)
        JavaPairRDD<String, String> urlWords = pages.flatMapToPair(x -> {
            ArrayList<Tuple2<String,String>> pairs = new ArrayList<Tuple2<String,String>>();
            String curStr = x._2.replace(",", "");
            curStr = curStr.replace(".", "");

            String[] words = curStr.split(" ");
            for (String word : words) {
                if (word.length() > 1)
                    pairs.add(new Tuple2<>(word, x._1));
            }
            return pairs.listIterator();
        });



        JavaPairRDD<String, String> matches = urlWords.filter(x -> keywordsStr.contains(x._1));

        urlWords.foreach(x-> System.out.println(x));

        matches.foreach(x -> System.out.println(x._1));
        matches.foreach(x -> System.out.println(x._2));
    }
}
