package gr.athena.innovation.fagi.merger;

/**
 * Wrapper exception for wrong input related exceptions.
 * 
 * @author nkarag
 */
public class WrongInputException extends Exception{

      public WrongInputException() {}

      public WrongInputException(String message){
         super(message);
      }
}

