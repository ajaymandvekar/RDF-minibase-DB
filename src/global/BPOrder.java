package global;

/** 
 * Enumeration class for BPOrder
 * 
 */

public class BPOrder {

  public static final int Ascending  = 0;
  public static final int Descending = 1;
  public static final int Random     = 2;

  public int bpOrder;

  /** 
   * BPOrder Constructor
   * <br>
   * A basic pattern ordering can be defined as 
   * <ul>
   * <li>   BPOrder bpOrder = new BPOrder(BPOrder.Random);
   * </ul>
   * and subsequently used as
   * <ul>
   * <li>   if (bpOrder.tupleOrder == BPOrder.Random) ....
   * </ul>
   *
   * @param _bpOrder The possible ordering of the tuples 
   */

  public BPOrder (int _bpOrder) {
	  bpOrder = _bpOrder;
  }

  public String toString() {
    
    switch (bpOrder) {
    case Ascending:
      return "Ascending";
    case Descending:
      return "Descending";
    case Random:
      return "Random";
    }
    return ("Unexpected TupleOrder " + bpOrder);
  }

}