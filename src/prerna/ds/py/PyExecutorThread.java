package prerna.ds.py;

import java.util.Hashtable;

import jep.Jep;
import jep.JepConfig;
import jep.JepException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.util.Constants;
import prerna.util.DIHelper;

public class PyExecutorThread extends Thread {

	private static final Logger LOGGER = LogManager.getLogger(PyExecutorThread.class.getName());
	
	private Jep jep = null;
	private Object daLock = new Object();
	
	public String [] command = null;
	public Hashtable <String, Object> response = new Hashtable<String, Object>();
	
	private volatile boolean keepAlive = true;
	private volatile boolean ready = false;

	@Override
	public void run() {
		// wait to see if process is true
		// if process is true - process, put the result and go back to sleep
		LOGGER.info("Running Python thread");
		getJep();

		while(this.keepAlive) {
			try {
				synchronized(daLock) {
					LOGGER.info("Waiting for next command");
					ready = true;
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
						    		response.put(thisCommand, "");
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
	
	public boolean isReady() {
		return ready;
	}
	
	public Object getMonitor() {
		return daLock;
	}
	
	public Jep getJep() {
		try {
			if(this.jep == null) {
				
				//https://github.com/ninia/jep/issues/140
				JepConfig aJepConfig = new JepConfig();
				aJepConfig.addSharedModules("pandas");		
				aJepConfig.addSharedModules("numpy");
				aJepConfig.addSharedModules("sys");
				aJepConfig.addSharedModules("fuzzywuzzy");
				aJepConfig.addSharedModules("string");
				aJepConfig.addSharedModules("random");
				aJepConfig.addSharedModules("datetime");
				aJepConfig.addSharedModules("annoy");
				
				
				// add the sys.path to python libraries for semoss
				String pyBase = null;
				pyBase = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/" + Constants.PY_BASE_FOLDER;
				//pyBase = "c:/users/pkapaleeswaran/workspacej3/SemossWeb/py";
				pyBase = pyBase.replaceAll("\\\\", "/");

				// add the include path
				aJepConfig.addIncludePaths(pyBase);
				String sitepackages = DIHelper.getInstance().getProperty("PYTHON_PACKAGES");
				aJepConfig.addIncludePaths(sitepackages);
				

				
				jep = new Jep(aJepConfig);

				jep.eval("import numpy as np");
				jep.eval("import pandas as pd");
				jep.eval("import gc as gc");
				jep.eval("import sys");
				jep.eval("from fuzzywuzzy import fuzz");
				jep.eval("import pandas as pd");
				jep.eval("import string");
				jep.eval("import random");
				jep.eval("import datetime");
				jep.eval("from annoy import AnnoyIndex");
				jep.eval("import numpy");
				jep.eval("import sys");
				System.err.println("Adding Syspath " + pyBase);				
				jep.eval("sys.path.append('" + pyBase + "')" );

				System.out.println(jep.getValue("sys.path"));
				
				jep.eval("from clean import PyFrame");
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
