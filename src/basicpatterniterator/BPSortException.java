package basicpatterniterator;

import java.lang.*;
import chainexception.*;

public class BPSortException extends ChainException 
{
  public BPSortException(String s) {super(null,s);}
  public BPSortException(Exception e, String s) {super(e,s);}
}
