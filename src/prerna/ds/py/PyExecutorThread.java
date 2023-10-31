package prerna.ds.py;

import java.util.Hashtable;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jep.Jep;
import jep.JepConfig;
import jep.JepException;
import jep.SharedInterpreter;
import jep.python.PyObject;
import prerna.sablecc2.ReactorSecurityManager;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.util.Constants;
import prerna.util.DIHelper;

public final class PyExecutorThread extends Thread {

	private static final String CLASS_NAME = PyExecutorThread.class.getName();
	private static final Logger classLogger = LogManager.getLogger(CLASS_NAME);
	
	private static transient SecurityManager defaultManager = System.getSecurityManager();

	private static boolean first = true;
	private Jep jep = null;
	private Object daLock = new Object();
	ThreadState curState = ThreadState.init;

	public String[] command = null;
	public Hashtable<String, Object> response = new Hashtable<>();

	private volatile boolean keepAlive = true;
	private volatile boolean ready = false;
	private Object driverMonitor = null;
	
	@Override
	public void run() {
		// wait to see if process is true
		// if process is true - process, put the result and go back to sleep
		classLogger.debug("JEP Thread STARTED");
		getJep();

		while (this.keepAlive) {
			try {
				synchronized (daLock) {
					
					classLogger.debug("Waiting for next command");
					ready = true;
					
					if(command == null || command.length == 0)
					{
						curState = ThreadState.wait;
						daLock.wait();
						response.clear();
					}
					
					curState = ThreadState.run;
					// if someone wakes up
					// process the command
					// set the response go back to sleep
					if (this.keepAlive) {
						ReactorSecurityManager tempManager = new ReactorSecurityManager();
						tempManager.addClass(CLASS_NAME);
						System.setSecurityManager(tempManager);

						for (int cmdLength = 0; command != null && cmdLength < command.length; cmdLength++) {
							String thisCommand = command[cmdLength];
							while (thisCommand.endsWith(";"))
								thisCommand = thisCommand.substring(0, thisCommand.length() - 1);

							Object thisResponse = null;
							try {
								classLogger.debug(">>>>>>>>>>>");
								classLogger.info("Executing Command .. " + thisCommand);
								classLogger.debug("<<<<<<<<<<<");
								try {
									thisResponse = jep.getValue(thisCommand);
								} catch (Exception ex) 
								{
									try
									{
										jep.eval(thisCommand);
									} catch(JepException ex2)
									{
										// use the exception as a response if we threw with the callback
										if(ex2.getCause() instanceof PythonExceptionWrapper) {
											thisResponse = new SemossPixelException(ex2.getCause().getMessage());
										} else {
											ex2.printStackTrace();
										}
									} catch(Exception ex2) 
									{
										ex2.printStackTrace();
									}
								} finally
								{
									if(thisResponse == null)
										thisResponse = "";
									response.put(thisCommand, thisResponse);
								}

								daLock.notify();
								if (driverMonitor != null) {
									synchronized (driverMonitor) {
										driverMonitor.notify();
										driverMonitor = null;
									}
								}
								// seems like when there is an exception..I need to restart the thread
							} catch (Exception e) {
								try {
									daLock.notify();
									if (driverMonitor != null) {
										synchronized (driverMonitor) {
											driverMonitor.notify();
											driverMonitor = null;
										}
									}
								} catch (Exception e1) {
									classLogger.error(Constants.STACKTRACE, e);
								}
								classLogger.error(Constants.STACKTRACE, e);
							}
						}
						command = null;

						// set back the original security manager
						tempManager.removeClass(CLASS_NAME);
						System.setSecurityManager(defaultManager);
					}
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
		
		if(jep != null) {
			try {
				jep.close();
			} catch (JepException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
		classLogger.info("JEP Thread ENDED");
	}

	public void setDriverMonitor(Object driverMonitor) {
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
			if (this.jep == null) {
				// forcing a sleep here
				/*
					try {
						Thread.sleep(20000);
					}catch(Exception ignored)
					{
						
					}
				*/
				
				JepConfig aJepConfig = new JepConfig();
				// this is not longer needed in 3.9.0
				// https://github.com/ninia/jep/issues/140
				/*aJepConfig.addSharedModules("pandas", 
						"numpy",
						"sys", 
						"fuzzywuzzy", 
						"string", 
						"random", 
						"datetime", 
						"annoy",
						"sklearn",
						"pulp");*/

				// add the sys.path to python libraries for semoss
				String pyBase = null;
				pyBase = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/" + Constants.PY_BASE_FOLDER;
				pyBase = pyBase.replace('\\', '/');
				aJepConfig.addIncludePaths(pyBase);
				
//				try {
//					aJepConfig.redirectStdout(new FileOutputStream("c:/temp/pyout.out"));
//				} catch (FileNotFoundException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
				
				aJepConfig.setRedirectOutputStreams(true);

				// add the libraries
				String sitepackages = DIHelper.getInstance().getProperty("PYTHON_PACKAGES");
				if (sitepackages != null && !sitepackages.isEmpty()) {
					aJepConfig.addIncludePaths(sitepackages);
				}
				initSharedInterpreter(aJepConfig);
				
				jep = new SharedInterpreter();

				//jep.eval("from jep import redirect_streams");
				//jep.eval("redirect_streams.setup()");
				
				// exec(open('c:/users/pkapaleeswaran/workspacej3/SemossDev/py/init.py').read())
				String execCommand = "exec(open('" + pyBase + "/init.py" + "').read())";
				jep.eval(execCommand);
				
				/*
				jep.eval("import numpy as np");
				jep.eval("import pandas as pd");
				jep.eval("import gc as gc");
				jep.eval("import sys");
				// workaround for issue with matplotlib.pyplot.plot() not working with python
				// 3.7.3; sys.argv is assumed to have length > 0
				// see https://github.com/ninia/jep/issues/187 for details
				// do it only if the version is there and it is 3.6
				long major = (Long)jep.getValue("sys.version_info[0]");
				long minor = (Long)jep.getValue("sys.version_info[1]");
				//System.err.println("VERSION OF PYTHON IS " + major + "." + minor);
				if(major >= 3 && minor >= 7)
				{
					//System.err.println("VERSION OF PYTHON IS " + major + "." + minor);
					jep.eval("sys.argv.append('')"); 
				}
				jep.eval("from fuzzywuzzy import fuzz");
				jep.eval("import string");
				jep.eval("import random");
				jep.eval("import datetime");
				// jep.eval("from annoy import AnnoyIndex");
				*/

				classLogger.debug("Adding Syspath " + pyBase);
				jep.eval("sys.path.append('" + pyBase + "')");
				classLogger.debug(jep.getValue("sys.path"));

				// these needs to be a better way to do this where we can add other things
				jep.eval("from clean import PyFrame");
				jep.eval("import smssutil");
				
				// include a callback to throw encountered exceptions
				jep.set("pyExecutorThread", PyExecutorThread.class);
				jep.eval("smssutil.setExecutorExceptionCallback(pyExecutorThread)");
			}
		} catch (JepException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		return jep;
	}
	
	public void killThread() {
		this.keepAlive = false;
	}

	/**
	 * Making init thread safe
	 * 
	 * @param aJepConfig
	 * @throws JepException
	 */
	private void initSharedInterpreter(JepConfig aJepConfig) throws JepException {
		if (!first) {
			return;
		}

		synchronized (this) {
			if (first) {
				SharedInterpreter.setConfig(aJepConfig);
				first = false;
			}
		}
	}
	
	public static class PythonExceptionWrapper extends RuntimeException {
		private static final long serialVersionUID = 1L;
		public final PyObject cause;

        public PythonExceptionWrapper(String message, PyObject cause) {
            super(message);
            this.cause = cause;
        }
    }
	
	public static void throwPython(String message, PyObject cause) {
        throw new PythonExceptionWrapper(message, cause);
    }
	
	
	/***** PURELY FOR TESTING PURPOSES
	 
	public void makeTheCall(PyTester pt)
	{
		try {
			getJep().eval("print(et.startSession('monkesh'))");
		} catch (JepException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void makeAnotherCall(PyTester pt)
	{
		et.setPyTester(pt);
		try {
			getJep().eval("print(et.startSession('monkesh'))");
		} catch (JepException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	*************************/
}
