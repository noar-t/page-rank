public class PageRank {
  public static class Aggregate {
    String dest;
    double pr;
    double outgoing;

    public Aggregate(String dest, double pr, double outgoing) {
      this.dest = dest;
      this.pr = pr;
      this.outgoing = outgoing;
    }
  }
}
