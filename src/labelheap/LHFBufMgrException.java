package labelheap;
import chainexception.*;

public class LHFBufMgrException extends ChainException{


  public LHFBufMgrException()
  {
     super();
  
  }

  public LHFBufMgrException(Exception ex, String name)
  {
    super(ex, name);
  }

}
