package tripleiterator;

import java.lang.*;
import chainexception.*;

public class TripleSortException extends ChainException 
{
  public TripleSortException(String s) {super(null,s);}
  public TripleSortException(Exception e, String s) {super(e,s);}
}
