/* File hferr.java  */

package labelheap;
import chainexception.*;

public class LHFException extends ChainException{


  public LHFException()
  {
     super();
  
  }

  public LHFException(Exception ex, String name)
  {
    super(ex, name);
  }



}