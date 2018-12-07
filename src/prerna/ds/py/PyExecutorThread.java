package prerna.ds.py;

import java.util.Hashtable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import jep.Jep;
import jep.JepException;

public class PyExecutorThread extends Thread {

	private static final Logger LOGGER = LogManager.getLogger(PyExecutorThread.class.getName());
	
	private Jep jep = null;
	private Object daLock = new Object();
	
	public String [] command = null;
	public Hashtable <String, Object> response = new Hashtable<String, Object>();
	
	private boolean keepAlive = true;

	@Override
	public void run() {
		// wait to see if process is true
		// if process is true - process, put the result and go back to sleep
		getJep();
		while(this.keepAlive) {
			try {
				synchronized(daLock) {
					LOGGER.info("Waiting for next command");
					daLock.wait();
					
					// if someone wakes up
					// process the command
					// set the response go back to sleep
					if(this.keepAlive) {
						for(int cmdLength = 0;cmdLength < command.length;cmdLength++) {
							String thisCommand = command[cmdLength];
							Object thisResponse = null;
						    try {
						    	LOGGER.info(">>>>>>>>>>>");
						    	LOGGER.info("Executing Command .. " + thisCommand);
						    	LOGGER.info("<<<<<<<<<<<");
						    	try {
						    		thisResponse = jep.getValue(thisCommand);
						    		response.put(thisCommand, thisResponse);
						    	}catch (Exception ex) {
						    		jep.eval(thisCommand);
						    		//response.put(thisCommand, "");
						    	}
								daLock.notify();
								
								// seems like when there is an exception..I need to restart the thread
							} catch (Exception e) {
								try {
									daLock.notify();
								} catch (Exception e1) {
									e1.printStackTrace();
								}
								e.printStackTrace();
							}
						}
					}
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		LOGGER.info("Thread ENDED");
	}
	
	public Object getMonitor() {
		return daLock;
	}
	
	public Jep getJep() {
		try {
			if(this.jep == null) {
				jep = new Jep(false);
			}
		} catch (JepException e) {
			e.printStackTrace();
		}
		return jep;
	}
	
	public void killThread() {
		this.keepAlive = false;
	}

}
