package tripleiterator;

import chainexception.*;
import java.lang.*;

public class TripleIteratorBMException extends ChainException 
{
  public TripleIteratorBMException(String s){super(null,s);}
  public TripleIteratorBMException(Exception prev, String s){ super(prev,s);}
}
