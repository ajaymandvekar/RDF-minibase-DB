package tripleiterator;

/**
 * An element in the binary tree.
 * including pointers to the children, the parent in addition to the item.
 */
public class TriplepnodeSplayNode
{
  /** a reference to the element in the node */
  public Triplepnode       item;

  /** the left child pointer */
  public TriplepnodeSplayNode    lt;

  /** the right child pointer */
  public TriplepnodeSplayNode    rt;

  /** the parent pointer */
  public TriplepnodeSplayNode    par;

  /**
   * class constructor, sets all pointers to <code>null</code>.
   * @param h the element in this node
   */
  public TriplepnodeSplayNode(Triplepnode h) 
  {
    item = h;
    lt = null;
    rt = null;
    par = null;
  }

  /**
   * class constructor, sets all pointers.
   * @param h the element in this node
   * @param l left child pointer
   * @param r right child pointer
   */  
  public TriplepnodeSplayNode(Triplepnode h, TriplepnodeSplayNode l, TriplepnodeSplayNode r) 
  {
    item = h;
    lt = l;
    rt = r;
    par = null;
  }

  /** a static dummy node for use in some methods */
  public static TriplepnodeSplayNode dummy = new TriplepnodeSplayNode(null);
  
}

