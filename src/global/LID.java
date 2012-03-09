/*  File LID.java   */

package global;

import java.io.*;

/** class LID
 */

public class LID{
  
  /** public int slotNo
   */
  public int slotNo;
  
  /** public PageId pageNo
   */
  public PageId pageNo = new PageId();
  
  /**
   * default constructor of class
   */
  public LID () { }
  
  /**
   *  constructor of class
   */
  public LID (PageId pageno, int slotno)
    {
      pageNo = pageno;
      slotNo = slotno;
    }
  
  /**
   * make a copy of the given lid
   */
  public void copyLid (LID lid)
    {
      pageNo = lid.pageNo;
      slotNo = lid.slotNo;
    }
  
  /** Write the lid into a byte array at offset
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
  
  
  /** Compares two LID object, i.e, this to the lid
   * @param lid LID object to be compared to
   * @return true is they are equal
   *         false if not.
   */
  public boolean equals(LID lid) {
    
    if ((this.pageNo.pid==lid.pageNo.pid)
	&&(this.slotNo==lid.slotNo))
      return true;
    else
      return false;
  }

  /** Returns the corresponding EID
   * @return EID
   */
  public EID returnEID() {
  
    EID eid = new EID(this);
    return eid;

  }
  
  /** Returns the corresponding PID
   * @return PID
   */
  public PID returnPID() {
  
    PID pid = new PID(this);
    return pid;

  }
}
