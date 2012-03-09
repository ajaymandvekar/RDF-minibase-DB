package tripleiterator;

import global.*;
import bufmgr.*;
import diskmgr.*;
import heap.*;
import java.io.*;
import tripleheap.*;

/**
 * Implements a sorted binary tree for triples.
 * abstract methods <code>enq</code> and <code>deq</code> are used to add 
 * or remove elements from the tree.
 */  
public abstract class TriplepnodePQ
{
  /** number of elements in the tree */
  protected int count;

  /** the sorting order (on subject,object,predicate,confidence) */
  protected TripleOrder  sort_order;

  /**
   * class constructor, set <code>count</code> to <code>0</code>.
   */
  public TriplepnodePQ() { count = 0; } 

  /**
   * returns the number of elements in the tree.
   * @return number of elements in the tree.
   */
  public int length(){ return count; }

  /** 
   * tests whether the tree is empty
   * @return true if tree is empty, false otherwise
   */
  public boolean empty() { return count == 0; }
  

  /**
   * insert an triple element in the tree in the correct order.
   * @param item the element to be inserted
   * @exception IOException from lower layers
   * @exception TripleUtilsException error in triple compare routines
   */
  abstract public void  Tripleenq(Triplepnode  item)
  throws IOException, UnknowAttrType, TripleUtilsException;

  /**
   * removes the minimum (Ascending)
   * from the tree.
   * @return the element removed, null if the tree is empty
   */
  abstract public Triplepnode Tripledeq();
	
  /**
   * compares two elements.
   * @param a one of the element for comparison
   * @param b the other element for comparison
   * @return  <code>0</code> if the two are equal,
   *          <code>1</code> if <code>a</code> is greater,
   *         <code>-1</code> if <code>b</code> is greater
   * @exception IOException from lower layers
   * @exception TripleUtilsException error in triple compare routines
   */
  public int TriplepnodeCMP(Triplepnode a, Triplepnode b) 
  throws IOException, UnknowAttrType, TripleUtilsException
  {
    int ans = TripleUtils.CompareTripleWithTriple(sort_order, a.triple, b.triple);
    return ans;
  }

  /**
   * tests whether the two elements are equal.
   * @param a one of the element for comparison
   * @param b the other element for comparison
   * @return <code>true</code> if <code>a == b</code>,
   *         <code>false</code> otherwise
   * @exception IOException from lower layers
   * @exception TripleUtilsException error in triple compare routines
   */  
  public boolean TriplepnodeEQ(Triplepnode a, Triplepnode b) 
  throws IOException, UnknowAttrType, TripleUtilsException
  {
    return TriplepnodeCMP(a, b) == 0;
  }
  
}
