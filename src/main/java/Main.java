

public class Main {

    public static void main(String[] args) {
      System.out.println("main");
      //PageRank.main(new String[0]);

      System.out.println("lite version");

      for (int i = 1; i < 21; i ++) {
          String[] arg = {"" + i};
          PageRankLite.main(arg);
      }

    }
}
