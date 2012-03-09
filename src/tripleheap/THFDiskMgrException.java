/* File hferr.java  */

package tripleheap;
import chainexception.*;

public class THFDiskMgrException extends ChainException{


  public THFDiskMgrException()
  {
     super();
  
  }

  public THFDiskMgrException(Exception ex, String name)
  {
    super(ex, name);
  }

}
