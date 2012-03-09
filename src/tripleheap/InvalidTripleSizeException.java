package tripleheap;
import chainexception.*;

public class InvalidTripleSizeException extends ChainException{

   public InvalidTripleSizeException()
   {
      super();
   }
   
   public InvalidTripleSizeException(Exception ex, String name)
   {
      super(ex, name); 
   }

}

