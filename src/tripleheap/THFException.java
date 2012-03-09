package tripleheap;
import chainexception.*;

public class THFException extends ChainException{

	
	public THFException()
	  {
	     super();
	  
	  }

	  public THFException(Exception ex, String name)
	  {
	    super(ex, name);
	  }


}
