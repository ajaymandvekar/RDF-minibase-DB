package tripleiterator;

import chainexception.*;

import java.lang.*;

public class TripleUtilsException extends ChainException {
  public TripleUtilsException(String s){super(null,s);}
  public TripleUtilsException(Exception prev, String s){ super(prev,s);}
}
