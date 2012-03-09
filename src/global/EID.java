/*  File EID.java   */

package global;

import java.io.*;

/** class EID
 */

public class EID{
  
  /** public int slotNo
   */
  public int slotNo;
  
  /** public PageId pageNo
   */
  public PageId pageNo = new PageId();
  
  /**
   * default constructor of class
   */
  public EID () { }
  
  /**
   *  constructor of class
   */
  public EID (LID lid)
    {
      pageNo = lid.pageNo;
      slotNo = lid.slotNo;
    }
  
  /**
   * make a copy of the given eid
   */
  public void copyEid (EID eid)
    {
      pageNo = eid.pageNo;
      slotNo = eid.slotNo;
    }
  
  /** Write the eid into a byte array at offset
   * @param ary the specified byte array
   * @param offset the offset of byte array to write 
   * @exception java.io.IOException I/O errors
   */ 
  public void writeToByteArray(byte [] ary, int offset)
    throws java.io.IOException
    {
      Convert.setIntValue ( slotNo, offset, ary);
      Convert.setIntValue ( pageNo.pid, offset+4, ary);
    }
  
  
  /** Compares two EID object, i.e, this to the eid
   * @param rid EID object to be compared to
   * @return true is they are equal
   *         false if not.
   */
  public boolean equals(EID eid) {
    
    if ((this.pageNo.pid==eid.pageNo.pid)
	&&(this.slotNo==eid.slotNo))
      return true;
    else
      return false;
  }

  
  /** Returns corresponding LID
   * @return LID
   */
  public LID returnLID() {
     
     LID lid = new LID(pageNo,slotNo);  
     return lid;
  }
  
}
