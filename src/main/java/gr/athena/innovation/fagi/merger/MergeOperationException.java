package gr.athena.innovation.fagi.merger;


/**
 * Wrapper exception for I/O exceptions regarding the merging process.
 * 
 * @author nkarag
 */
public class MergeOperationException extends Exception{

      public MergeOperationException() {}

      public MergeOperationException(String message){
         super(message);
      }
}
