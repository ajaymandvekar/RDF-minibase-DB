
package basicpatterniterator;

/**
 * An element in the binary tree.
 * including pointers to the children, the parent in addition to the item.
 */
public class BPpnodeSplayNode
{
  /** a reference to the element in the node */
  public BPpnode             item;

  /** the left child pointer */
  public BPpnodeSplayNode    lt;

  /** the right child pointer */
  public BPpnodeSplayNode    rt;

  /** the parent pointer */
  public BPpnodeSplayNode    par;

  /**
   * class constructor, sets all pointers to <code>null</code>.
   * @param h the element in this node
   */
  public BPpnodeSplayNode(BPpnode h) 
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
  public BPpnodeSplayNode(BPpnode h, BPpnodeSplayNode l, BPpnodeSplayNode r) 
  {
    item = h;
    lt = l;
    rt = r;
    par = null;
  }

  /** a static dummy node for use in some methods */
  public static BPpnodeSplayNode dummy = new BPpnodeSplayNode(null);
  
}

