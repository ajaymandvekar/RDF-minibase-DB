package btree;
import global.*;
//import btree.*;

/**  IndexData: It extends the DataClass.
 *   It defines the data "tid" for leaf node in B++ tree.
 */
public class TripleLeafData extends DataClass {
  private TID myTid;

  public String toString() {
     String s;
     s="[ "+ (new Integer(myTid.pageNo.pid)).toString() +" "
              + (new Integer(myTid.slotNo)).toString() + " ]";
     return s;
  }

  /** Class constructor
   *  @param    tid  the data tid
   */
  public TripleLeafData(TID tid) {myTid= new TID(tid.pageNo, tid.slotNo);};  

  /** get a copy of the tid
  *  @return the reference of the copy 
  */
  public TID getData() {return new TID(myTid.pageNo, myTid.slotNo);};

  /** set the tid
   */ 
  public void setData(TID tid) { myTid= new TID(tid.pageNo, tid.slotNo);};
}   
