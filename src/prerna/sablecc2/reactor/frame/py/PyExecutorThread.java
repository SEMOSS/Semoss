package prerna.sablecc2.reactor.frame.py;

import jep.Jep;
import jep.JepException;

public class PyExecutorThread extends Thread {

	public String process = "wait";
	boolean eval = true; 
	public String command = null;
	public Object response = null;
	
	Jep jep = null;
	Object daLock = new Object();
	
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		
		// wait to see if process is true
		// if process is true - process, put the result and go back to sleep
		
		getJep();
		
		while(!process.equalsIgnoreCase("stop"))
		{
			try {
				synchronized(daLock)
				{
					System.out.println("Waiting for next command");
					daLock.wait();
					
					// if someone wakes up
					// process the command
					// set the response go back to sleep
				    try {
				    	try
				    	{
				    		response = jep.getValue(command);
				    	}catch (Exception ex)
				    	{
				    		jep.eval(command);
				    		response = "";
				    	}
						daLock.notify();
						
						// seems like when there is an exception..I need to restart the thread
					} catch (Exception e) {
						try {
							daLock.notify();
						} catch (Exception e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
		}
		System.out.println("Thread ENDED");

	}
	
	public Object getMonitor()
	{
		return daLock;
	}
	
	public Jep getJep()
	{
		try {
			if(this.jep == null)
				jep = new Jep(false);
		} catch (JepException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return jep;

	}

}
