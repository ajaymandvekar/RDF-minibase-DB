/*  File TID.java   */

package global;

import java.io.*;

/** class TID
 */

public class TID{
  
  /** public int slotNo
   */
  public int slotNo;
  
  /** public PageId pageNo
   */
  public PageId pageNo = new PageId();
  
  /**
   * default constructor of class
   */
  public TID () { }
  
  /**
   *  constructor of class
   */
  public TID (PageId pageno, int slotno)
    {
      pageNo = pageno;
      slotNo = slotno;
    }
  
  /**
   * make a copy of the given rid
   */
  public void copyTid (TID rid)
    {
      pageNo = rid.pageNo;
      slotNo = rid.slotNo;
    }
  
  /** Write the tid into a byte array at offset
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
  
  
  /** Compares two TID object, i.e, this to the tid
   * @param tid TID object to be compared to
   * @return true is they are equal
   *         false if not.
   */
  public boolean equals(TID tid) {
    
    if ((this.pageNo.pid==tid.pageNo.pid)
	&&(this.slotNo==tid.slotNo))
      return true;
    else
      return false;
  }
  
}
