package prerna.pyserve;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.nustaq.serialization.FSTObjectOutput;

import jep.Jep;
import jep.JepConfig;
import jep.JepException;
import jep.SharedInterpreter;
import prerna.util.Constants;

public class FileBasedPyWorker extends Thread implements IWorker{
	
	// basically a process which works by looking for commands in file space
	private static final String CLASS_NAME = FileBasedPyWorker.class.getName();

	List <String> commandList = new ArrayList<String>(); // this is giving the file name and that too relative
	public static final Logger LOGGER = LogManager.getLogger(CLASS_NAME);

	String internalLock = "Internal Lock";
	private static boolean first = true;
	public Jep jep = null;
	
	public String [] command = null;
	public Hashtable <String, Object> response = new Hashtable<String, Object>();
	
	public volatile boolean keepAlive = true;
	public volatile boolean ready = false;
	public Object driverMonitor = null;
	
	Properties prop = null; // this is basically reference to the RDF Map
	List foldersBeingWatched = new ArrayList();
	List threads = new ArrayList();
	public String mainFolder = null;
	

	
	public static void main(String [] args)
	{
		
		
		// arg1 - the directory where commands would be thrown
		// arg2 - access to the rdf map to load
		
		// create the watch service
		// start this thread
		
		// when event comes write it to the command
		
		
		PropertyConfigurator.configure(args[0] + "/log4j.properties");
	
		FileBasedPyWorker worker = new FileBasedPyWorker();
		
		worker.prop = new Properties();
		try {
			worker.prop.load(new FileInputStream(args[1]));
			System.out.println("Loaded the rdf map");
			
			// get the library for jep
			//String jepLib = worker.prop.getProperty("JEP_LIB");
			
			//System.loadLibrary(jepLib);
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		worker.mainFolder = args[0];
		worker.watchDir(args[0]);
		((Thread)worker).start();
	}
	
	public void watchDir(String dir)
	{
		LOGGER.info("Adding new watch for .. " + dir);
		if(!foldersBeingWatched.contains(dir))
		{
			LOGGER.info("Creating File Thread " + dir);

			FileThread ft = new FileThread();
			ft.folderToWatch = dir;
			ft.setWorker(this);
			threads.add(ft);
			Thread fw = new Thread(ft);
			fw.start();
			// add to say this folder is being watched
			foldersBeingWatched.add(dir);
		}
	}
		
	public void printCP()
	{
		ClassLoader cl = ClassLoader.getSystemClassLoader();

        URL[] urls = ((URLClassLoader)cl).getURLs();

        for(URL url: urls){
        	LOGGER.info(url.getFile());
        }
	}
	@Override
	public void run() {
		// wait to see if process is true
		// if process is true - process, put the result and go back to sleep
		LOGGER.debug("Running Python thread");
		LOGGER.info("Classpath set to.. " );
		//printCP();
		getJep();
		this.ready = true;

		while(this.keepAlive) {
			try {
				//synchronized(daLock) //<-- remove this
				{
					
					// Experimental
					if(commandList.size() > 0)
					{
						this.command = new String[]{commandList.remove(0)};
						//command = thisPayload.getCommand();
					
						//experimental 
						
						LOGGER.debug("Waiting for next command");
						ready = true;
						response.clear();
						// if someone wakes up
						// process the command
						// set the response go back to sleep
						if(this.keepAlive) {
							
							
							for(int cmdLength = 0;cmdLength < command.length;cmdLength++) {
								String thisCommand = command[cmdLength];
								
								// this is the file name
								// I also need to create an output from it
								String scriptFile = thisCommand;
								//String outputFile = scriptFile + ".output";
								
								String execCommand = getCommand(scriptFile);
																
								Object thisResponse = null;
							    try {
							    	LOGGER.debug(">>>>>>>>>>>");
							    	LOGGER.info("Executing Command .. [" + execCommand + "]");
							    	LOGGER.debug("<<<<<<<<<<<");
							    	try {
							    		changeState(thisCommand, ".processing");
							    		LOGGER.info("Starting process of the command " + execCommand);
							    		thisResponse = jep.getValue(execCommand);
							    		if(thisResponse != null)
							    		{
							    			//LOGGER.info("Writing response " + response + " to a file " + scriptFile);
							    			writeObjectToFile(thisResponse, scriptFile);
							    		}
							    		else
							    		{
							    			LOGGER.info("Response is set to null " + response);
							    		}
							    		//response.put(thisCommand, thisResponse);
							    	}catch (Exception ex) {
							    		try {
							    			jep.eval(thisCommand);
							    			LOGGER.info("Eval Complete");
							    		}catch(Exception ex2)
							    		{
							    			processError(thisCommand, ex2);
							    			LOGGER.info(ex2);
							    		}
							    	}
						    		LOGGER.info("Get Value Complete");
						    		changeState(thisCommand, ".completed");
									// seems like when there is an exception..I need to restart the thread
								} catch (Exception e) {
									LOGGER.error(e);
								}
							}
							command = null;
							
						}
					}
					else if(keepAlive)
					{
						// lock on some shit and sleep
						synchronized(internalLock)
						{
							LOGGER.info("Sleeping now.. ");
							internalLock.wait();
							LOGGER.info("woke up ..Processing now..");
						}
						
					}
					else
					{
						
					}
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		LOGGER.debug("Thread ENDED");
	}
	
	private String getCommand(String scriptFile)
	{
		String retString = null;
		try
		{
			File scFile = new File(scriptFile);
			scFile.setWritable(false);
			retString = FileUtils.readFileToString(scFile);
			String input = retString;
			int newlineCount = 0;
			int index = input.indexOf("\n");
			while (index != -1) {
			    newlineCount++;
			    input = input.substring(index + 1);
			    index = input.indexOf("\n");
			}
			
			LOGGER.info("Number of new lines.. " + newlineCount);
			
			boolean multi = (newlineCount > 1) || 
							( retString.contains("=") && !retString.contains("groupby") ) || 
							(retString.contains(".") && retString.endsWith("()")) && !retString.equals("dir()"); 
			
			
			if(retString.startsWith("PY_DIRECT@@"))
			{
				multi = false;
				retString = retString.replace("PY_DIRECT@@", "");
				//retString = StringUtils.chomp(retString);
			}
			
			// make another provision for print
			if(retString.startsWith("print"))
			{
				multi=true;
			}
			if(multi)
			{
				String outputFile = scriptFile + ".output";
				retString = "smssutil.runwrapper(\"" + scriptFile + "\", \"" + outputFile + "\", \"" + outputFile + "\", globals())";
			}
			
			LOGGER.debug("Multi set " + multi + " For command " + retString);
		}catch(Exception ex)
		{
			LOGGER.error(ex);
		}
		return retString;
	}
	
	private void changeState(String fileName, String stateName)
	{
		try {
			LOGGER.info("Changing state of file " + fileName + " to " + stateName );
			String newState = fileName + ".state" + stateName;
			File daFile = new File(newState);
			if(!daFile.exists())
				daFile.createNewFile();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void processError(String fileName, Exception ex)
	{
		try {
			LOGGER.info("Processing error ");
			String outputFile = fileName + ".output";
			StringWriter sw = new StringWriter();
			ex.printStackTrace(new PrintWriter(sw));
			FileUtils.writeStringToFile(new File(outputFile), sw.toString());
			LOGGER.info("Errored with  " + ex.getMessage());
			changeState(fileName, ".completed");
			
		}catch(Exception ignored)
		{
			
		}
	}
	
	// process the file
	public void processCommand(String folderToWatch, String file)
	{
		try {
			String path = folderToWatch + "/" + file + ".state";
			File stateFile = new File(path);
			if(!stateFile.exists())
			{
				stateFile.createNewFile();
				commandList.add(folderToWatch + "/" + file);
				
				stateFile.renameTo(new File(path + ".initiated"));
				// inform the lock
				synchronized(internalLock)
				{
					internalLock.notify();
				}
			}				
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}

	
	// process the file
	public void processAdmin(String folderToWatch, String file)
	{
		try {
			String path = folderToWatch + "/" + file;
			File adminFile = new File(path);
			String command = FileUtils.readFileToString(adminFile).trim();
			
			// switch case block to deal with every command
			String [] cmdParts = command.split("@@");
			if(cmdParts[0].equalsIgnoreCase("addFolder"))
			{
				// need to start a new thread
				watchDir(cmdParts[1]);
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	public void processCleanup(String folderToWatch)
	{
		try
		{
			// see if this is the main directory
			if(folderToWatch.equalsIgnoreCase(this.mainFolder))
			{
				this.keepAlive = false;
				// wake up the other thread if it is sleeping
				synchronized(internalLock)
				{
					internalLock.notify();
				}
				
				// drop a close all on all the folders being watched
				for(int folderIndex = 0;folderIndex < foldersBeingWatched.size();folderIndex++)
				{
					File forceCloseFile = new File(foldersBeingWatched.get(folderIndex) + "/alldone.force_close");
					forceCloseFile.createNewFile();
				}
				FileUtils.deleteDirectory(new File(folderToWatch));					// kill the main folder
			}
			else
			{
				FileUtils.deleteDirectory(new File(folderToWatch));
				foldersBeingWatched.remove(folderToWatch);
			}
		}catch(Exception ex)
		{
			
		}
	}
	
	public void processCleanup(String folderToWatch, String file)
	{
		try {
			
			System.out.println("Finishing cleanup " + file);
			new File(folderToWatch + "/" + file).delete();
			String mainPyFile = file.replaceAll(".state.delivered", "");
			
			// I need to clean up the .state
			// and
			deleteState(folderToWatch, mainPyFile, ".state.processing");
			deleteState(folderToWatch, mainPyFile, ".state.completed");
			deleteState(folderToWatch, mainPyFile, ".state.initiated");
			deleteState(folderToWatch, mainPyFile, ".state");
			deleteState(folderToWatch, mainPyFile, ".isready");
			deleteState(folderToWatch, mainPyFile, ".ready");
			deleteState(folderToWatch, mainPyFile, ".SMSS_OBJ");

			/*
			String outputFileName = folderToWatch + "/" + mainPyFile + ".output";
			System.err.println("Deleting file.. " + outputFileName);
			File outputFile = new File(outputFileName);
			if(outputFile.exists())
				outputFile.delete();
			*/
			

			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	public void deleteState(String folderToWatch, String mainFile, String state)
	{
		String stateFileName = folderToWatch + "/" + mainFile + state;
		File stateFile = new File(stateFileName);
		if(stateFile.exists())
			stateFile.delete();
		
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
				pyBase = prop.getProperty(Constants.BASE_FOLDER) + "/" + Constants.PY_BASE_FOLDER;
				//pyBase = "c:/users/pkapaleeswaran/workspacej3/semossweb/py";
				pyBase = pyBase.replace('\\', '/');
				aJepConfig.addIncludePaths(pyBase);
				aJepConfig.setRedirectOutputStreams(false);
				
				// add the libraries
				String sitepackages = prop.getProperty("PYTHON_PACKAGES");
				if(sitepackages != null && !sitepackages.isEmpty()) {
					aJepConfig.addIncludePaths(sitepackages);
				}
				
				initSharedInterpreter(aJepConfig);
				jep = new SharedInterpreter();

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
	
	public void writeObjectToFile(Object obj, String scriptFile)
	{
		try {
			//LOGGER.info("Writing Object to.. " + scriptFile + obj);
			File outFile = new File(scriptFile + ".smss_obj");
			
			FileOutputStream fos = new FileOutputStream(outFile);
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			
			
			
			
			// FST SHIT just wont work
			// need to debug later
			
			// try 2
			/*
			FSTConfiguration conf = FSTConfiguration.createDefaultConfiguration();
			LOGGER.info("Configuration succeeded");			
			
			System.out.println(">>");
			byte barray[] = conf.asByteArray(obj);
			bos.write(barray);
			LOGGER.info("Simple as byte succeeded");
			*/
			
			LOGGER.info("Starting write to FST");
			FSTObjectOutput fo = new FSTObjectOutput(bos);
			fo.writeObject(obj);
			LOGGER.info("Completed write to FST");
			fo.close();
			
			//ObjectOutputStream oos = new ObjectOutputStream(fos);
			//oos.writeObject(obj);			
			bos.writeTo(fos);
			bos.close();
			//oos.close();
			fos.close();
			LOGGER.info("Completed write to FOS");
			
			LOGGER.info("Completed writing object file");

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			LOGGER.error(e);
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LOGGER.error(e);
			e.printStackTrace();
		}
		
	}




}
