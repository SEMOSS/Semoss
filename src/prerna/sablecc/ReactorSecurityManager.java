package prerna.sablecc;

import java.io.FileDescriptor;
import java.security.Permission;

public class ReactorSecurityManager extends SecurityManager {
	
    public void checkPermission( Permission permission ) {
        if( "exitVM".equals( permission.getName() ) ) {
          throw new SecurityException("Hello World") ;
        	//System.out.println("No Exit baby.. ");
        }
      }
    
    // remove exit
      public void checkExit( int status) {
	        //if( "exitVM".equals( permission.getName() ) ) {
    	  throw new SecurityException("Exit not permitted") ;	        	
	        //}
	   }

      // remove exec
      public void checkExec(String cmd)
      {
    	  throw new SecurityException("Exec not permitted " + cmd) ;	        	
    	  
      }
      
      /*public void checkPropertyAccess(String key)
      {
    	  throw new SecurityException("Property lookup not permitted.. " + key) ;	        	    	  
      }*/
	
    /*  public void checkRead(String file)
      {
    	  throw new SecurityException("File access not permitted.. " ) ;	        	    	      	  
      }

      public void checkRead(FileDescriptor desc)
      {
    	  throw new SecurityException("File access not permitted.. " ) ;	        	    	      	  
      }

      public void checkWrite(String file)
      {
    	  throw new SecurityException("File access not permitted.. " ) ;	        	    	      	  
      }

      public void checkWrite(FileDescriptor desc)
      {
    	  throw new SecurityException("File access not permitted.. " ) ;	        	    	      	  
      }
*/}
