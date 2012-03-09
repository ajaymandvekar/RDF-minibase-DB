/*  File PID.java   */

package global;

import java.io.*;

/** class PID
 */

public class PID{
  
  /** public int slotNo
   */
  public int slotNo;
  
  /** public PageId pageNo
   */
  public PageId pageNo = new PageId();
  
  /**
   * default constructor of class
   */
  public PID () { }
  
  /**
   *  constructor of class
   */
  public PID (LID lid)
    {
      pageNo = lid.pageNo;
      slotNo = lid.slotNo;
    }
  
  /**
   * make a copy of the given pid
   */
  public void copyPid (PID pid)
    {
      pageNo = pid.pageNo;
      slotNo = pid.slotNo;
    }
  
  /** Write the pid into a byte array at offset
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
  
  
  /** Compares two PID object, i.e, this to the pid
   * @param pid PID object to be compared to
   * @return true is they are equal
   *         false if not.
   */
  public boolean equals(PID pid) {
    
    if ((this.pageNo.pid==pid.pageNo.pid)
	&&(this.slotNo==pid.slotNo))
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
