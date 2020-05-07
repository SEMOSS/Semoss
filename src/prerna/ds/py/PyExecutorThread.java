package prerna.ds.py;

import java.util.Hashtable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import jep.Jep;
import jep.JepConfig;
import jep.JepException;
import jep.SharedInterpreter;
import prerna.sablecc.ReactorSecurityManager;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class PyExecutorThread extends Thread {

	private static final String CLASS_NAME = PyExecutorThread.class.getName();
	private static final Logger LOGGER = LogManager.getLogger(CLASS_NAME);
	private static transient SecurityManager defaultManager = System.getSecurityManager();

	private static boolean first = true;
	private Jep jep = null;
	private Object daLock = new Object();
	ThreadState curState = ThreadState.init;
	
	public String [] command = null;
	public Hashtable <String, Object> response = new Hashtable<String, Object>();
	
	public volatile boolean keepAlive = true;
	private volatile boolean ready = false;
	private Object driverMonitor = null;

	@Override
	public void run() {
		// wait to see if process is true
		// if process is true - process, put the result and go back to sleep
		LOGGER.debug("Running Python thread");
		getJep();

		while(this.keepAlive) {
			try {
				synchronized(daLock) {
					LOGGER.debug("Waiting for next command");
					ready = true;
					curState = ThreadState.wait;
					daLock.wait();
					response.clear();
					
					curState = ThreadState.run;
					// if someone wakes up
					// process the command
					// set the response go back to sleep
					if(this.keepAlive) {
						
						ReactorSecurityManager tempManager = new ReactorSecurityManager();
						tempManager.addClass(CLASS_NAME);
						System.setSecurityManager(tempManager);
						
						for(int cmdLength = 0;command != null && cmdLength < command.length;cmdLength++) {
							String thisCommand = command[cmdLength];
							while(thisCommand.endsWith(";"))
								thisCommand = thisCommand.substring(0, thisCommand.length()-1);

							Object thisResponse = null;
						    try {
						    	LOGGER.debug(">>>>>>>>>>>");
						    	LOGGER.info("Executing Command .. " + thisCommand);
						    	LOGGER.debug("<<<<<<<<<<<");
						    	try {
						    		thisResponse = jep.getValue(thisCommand);
						    		response.put(thisCommand, thisResponse);
						    	}catch (Exception ex) {
						    		jep.eval(thisCommand);
						    		response.put(thisCommand, "");
						    	}
						    	
						    	//jep.eval("from java.lang import System");
						    	//jep.eval("System.out.println('.')");

						    	daLock.notify();
						    	if(driverMonitor != null)
						    	{
						    		synchronized(driverMonitor)
						    		{
						    			driverMonitor.notify();
						    			driverMonitor = null;
						    		}
						    	}
								// seems like when there is an exception..I need to restart the thread
							} catch (Exception e) {
								try {
									daLock.notify();
							    	if(driverMonitor != null)
							    	{
							    		synchronized(driverMonitor)
							    		{
							    			driverMonitor.notify();
							    			driverMonitor = null;
							    		}
							    	}
								} catch (Exception e1) {
									e1.printStackTrace();
								}
								e.printStackTrace();
							}
						}
						command = null;
						
						// set back the original security manager
						tempManager.removeClass(CLASS_NAME);
						System.setSecurityManager(defaultManager);	
					}
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		LOGGER.debug("Thread ENDED");
	}
	
	public void setDriverMonitor(Object driverMonitor)
	{
		this.driverMonitor = driverMonitor;
	}
	
	public boolean isReady() {
		return ready;
	}
	
	public Object getMonitor() {
		return daLock;
	}
	
	public Jep getJep() {
		try {
			if(this.jep == null) {
				JepConfig aJepConfig = new JepConfig();
				// this is not longer needed in 3.9.0
				//https://github.com/ninia/jep/issues/140
//				aJepConfig.addSharedModules("pandas", 
//						"numpy",
//						"sys", 
//						"fuzzywuzzy", 
//						"string", 
//						"random", 
//						"datetime", 
//						"annoy",
//						"sklearn",
//						"pulp");
				
				// add the sys.path to python libraries for semoss
				String pyBase = null;
				pyBase = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/" + Constants.PY_BASE_FOLDER;
				//pyBase = "c:/users/pkapaleeswaran/workspacej3/semossweb/py";
				pyBase = pyBase.replace('\\', '/');
				aJepConfig.addIncludePaths(pyBase);
				aJepConfig.setRedirectOutputStreams(true);
				
				// add the libraries
				String sitepackages = DIHelper.getInstance().getProperty("PYTHON_PACKAGES");
				if(sitepackages != null && !sitepackages.isEmpty()) {
					aJepConfig.addIncludePaths(sitepackages);
				}
				
				initSharedInterpreter(aJepConfig);
				jep = new SharedInterpreter();
				
				jep.eval("from jep import redirect_streams");
				jep.eval("redirect_streams.setup()");

				jep.eval("import numpy as np");
				jep.eval("import pandas as pd");
				jep.eval("import gc as gc");
				jep.eval("import sys");
				jep.eval("from fuzzywuzzy import fuzz");
				jep.eval("import pandas as pd");
				jep.eval("import string");
				jep.eval("import random");
				jep.eval("import datetime");
				//jep.eval("from annoy import AnnoyIndex");
				jep.eval("import numpy");
				jep.eval("import sys");
				// workaround for issue with matplotlib.pyplot.plot() not working with python 3.7.3; sys.argv is assumed to have length > 0
				// see https://github.com/ninia/jep/issues/187 for details
				//jep.eval("sys.argv.append('')");
				// this is so we do not get a GIL
				//jep.eval("from java.lang import System");
				
				LOGGER.debug("Adding Syspath " + pyBase);				
				jep.eval("sys.path.append('" + pyBase + "')" );
				LOGGER.debug(jep.getValue("sys.path"));
				
				// these needs to be a better way to do this where we can add other things 
				jep.eval("from clean import PyFrame");
				jep.eval("import smssutil");
			}
		} catch (JepException e) {
			e.printStackTrace();
		}
		return jep;
	}
	
	public void killThread() {
		this.keepAlive = false;
	}

	/**
	 * Making init thread safe
	 * @param aJepConfig
	 * @throws JepException
	 */
	private void initSharedInterpreter(JepConfig aJepConfig) throws JepException {
		if(first) {
			synchronized (this) {
				if(first) {
					SharedInterpreter.setConfig(aJepConfig);
					first = false;
				}
			}
		}
	}
	
}
