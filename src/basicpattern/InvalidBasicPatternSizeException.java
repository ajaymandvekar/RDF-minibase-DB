package basicpattern;
import chainexception.*;

public class InvalidBasicPatternSizeException extends ChainException{

   public InvalidBasicPatternSizeException()
   {
      super();
   }
   
   public InvalidBasicPatternSizeException(Exception ex, String name)
   {
      super(ex, name); 
   }

}

