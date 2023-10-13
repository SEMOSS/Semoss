package prerna.tcp;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openqa.selenium.chrome.ChromeDriver;
import org.rosuda.REngine.Rserve.RConnection;

import prerna.ds.py.PyExecutorThread;
import prerna.ds.py.PyTranslator;
import prerna.engine.api.IDatabaseEngine;
import prerna.om.Insight;
import prerna.project.impl.Project;
import prerna.reactor.IReactor;
import prerna.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.reactor.frame.r.util.RJavaJriTranslator;
import prerna.reactor.frame.r.util.RJavaRserveTranslator;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.CmdExecUtil;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.FstUtil;
import prerna.util.TCPChromeDriverUtility;
import prerna.util.Utility;

public class SocketServerHandler implements Runnable {
	
	public static Logger classLogger = null;
	
	boolean test = false;
	
	private int offset = 4;
	private byte[] lenBytes = null;
	private byte[] curBytes = null;
	private int bytesReadSoFar = 0;
	private int lenBytesReadSoFar = 0;
	private boolean done = false;
	private boolean blocking = false; // processes one payload and moves to the next one. This is how it currently behaves
	private long averageMillis = 200;

	ServerSocket socket = null;
	SocketServer server = null;
	OutputStream os = null;
	InputStream is = null;
	String mainFolder = null;

	private PyExecutorThread pt = null;
	private PyTranslator pyt = null;
	
	private RConnection retCon = null;

	private Map <String, AbstractRJavaTranslator> rtMap = new HashMap<String, AbstractRJavaTranslator>();
	public Map <String, Insight> insightMap = new HashMap<String, Insight>();
	private Map <String, Project> projectMap = new HashMap<String, Project>();
	private Map <String, CmdExecUtil> cmdMap = new HashMap<String, CmdExecUtil>();
	
	private Map <String, PayloadStruct> incoming = new HashMap<String, PayloadStruct>();
	private Map <String, PayloadStruct> outgoing = new HashMap<String, PayloadStruct>();
	
	private int curEpoc = 1;
	
//	ErrorThread et = null;
		
	public void setLogger(Logger classLogger) {
		SocketServerHandler.classLogger = classLogger;
	}
	
	public void setPyExecutorThread(PyExecutorThread pt)
	{
		this.pt = pt;
		this.pyt = new PyTranslator();
		pyt.setPy(pt);
		// also start the error thread
		//startErrorThread();
	}
			
	// this is where the processing happens
	public PayloadStruct getFinalOutput(PayloadStruct ps)
	{
		try
		{				
			//System.err.println("Received For Processing " + ps.methodName +  "  bytes : " + totalBytes + " Epoc " + ps.epoc);
			//classLogger.info("Received For Processing " + ps.methodName +  "  bytes : " + totalBytes + " Epoc " + ps.epoc);
			//unprocessed.put(ps.epoc, ps);
			//attemptCount.put(ps.epoc, 1);

			incoming.put(ps.epoc, ps);
			ps.response = true;
			outgoing.put(ps.epoc, ps);
			
			//System.out.println("Getting final output for " + ps.methodName);
			classLogger.info("Getting final output for " + ps.methodName);
			
			////System.err.println("Payload set to " + ps);
			if(ps.methodName.equalsIgnoreCase("EMPTYEMPTYEMPTY")) { // trigger message ignore
				return ps;
			}
			if(ps.methodName.equalsIgnoreCase("CLOSE_ALL_LOGOUT<o>")) { // we are done kill everything
				cleanUp();
			}
			if(ps.methodName.equalsIgnoreCase("RELEASE_ALL")) { // we are done kill everything
				releaseAll();
				return ps;
			}
			
			if(ps.operation == PayloadStruct.OPERATION.R)
			{
				try {
					Method method = findRMethod(getTranslator(ps.env), ps.methodName, ps.payloadClasses);
					Object output = runMethodR(getTranslator(ps.env), method, ps.payload);
					if(output != null) {
						//System.out.println("Output is not null - R");
						classLogger.info("Output is not null - R");
					}
					Object [] retObject = new Object[1];
					retObject[0] = output;
					ps.payload = retObject;
					ps.processed = true;
					ps.response = true;
				} catch(InvocationTargetException ex) {
					classLogger.error(Constants.STACKTRACE, ex);
					classLogger.info(ex + ps.methodName);
					ex.printStackTrace();
					//System.err.println("Method.. " + ps.methodName);
					ps.ex = ExceptionUtils.getStackTrace(ex);						
				} catch(Exception ex ) {
					classLogger.error(Constants.STACKTRACE, ex);
					classLogger.info(ex + ps.methodName);
					ex.printStackTrace();
					//System.err.println("Method.. " + ps.methodName);
					ps.ex = ExceptionUtils.getStackTrace(ex);						
				}
				return ps;
			}
			if(ps.operation == PayloadStruct.OPERATION.PYTHON)
			{
				// get the py translator for the first time
				getPyTranslator();

				try {
					
					Method method = findPyMethod(ps.methodName, ps.payloadClasses);
					
					Object output = runMethodPy(method, ps.payload);
					Object [] retObject = new Object[1];
					retObject[0] = output;
					ps.payload = retObject;
					ps.processed = true;
					//ps.operation = ps.operation.PYTHON;
					// remove this item
				} catch(Exception ex) {
					ex.printStackTrace();
					classLogger.error(Constants.STACKTRACE, ex);
					//System.err.println("Method.. " + ps.methodName);
					ps.ex = ExceptionUtils.getStackTrace(ex);						
				}
				return ps;
			}
			else if(ps.operation == PayloadStruct.OPERATION.CHROME)
			{
				try {
					Method method = findChromeMethod(ps.methodName, ps.payloadClasses);
					Object output = runMethodChrome(method, ps.payload);
					if(output != null)
						classLogger.info("Output is not null - CHROME");
					if(output instanceof ChromeDriver)
						output = new Object();
					if(output instanceof String)
						classLogger.info("Output is >>>>>>>>>>>>>>>  " + output);
					Object [] retObject = new Object[1];
					retObject[0] = output;
					ps.payload = retObject;
					ps.processed = true;
				} catch(Exception ex) {
					classLogger.error(Constants.STACKTRACE, ex);
					//System.err.println("Method.. " + ps.methodName);
					ps.ex = ExceptionUtils.getStackTrace(ex);						
					//TCPChromeDriverUtility.quit("stop");
				}
				return ps;
			}
			else if(ps.operation == PayloadStruct.OPERATION.ECHO)
			{
				try {
					Method method = findChromeMethod(ps.methodName, ps.payloadClasses);
					Object output = ps.payload[0];
					Object [] retObject = new Object[1];
					retObject[0] = output;
					ps.payload = retObject;
					ps.processed = true;
				} catch(Exception ex) {
					classLogger.error(Constants.STACKTRACE, ex);
					//ex.printStackTrace();
					//System.err.println("Method.. " + ps.methodName);
					ps.ex = ExceptionUtils.getStackTrace(ex);						
					//TCPChromeDriverUtility.quit("stop");
				}
				return ps;
			}
			else if(ps.operation == PayloadStruct.OPERATION.INSIGHT)
			{
				try {
					Insight output = (Insight)ps.payload[0];
					output.setPyTranslator(pyt);
					if(output.getREnv() != null)
						output.setRJavaTranslator(rtMap.get(output.getREnv()));
					ps.payload = new Object[] {"Set insight successfully"};
					ps.payloadClasses = new Class[] {String.class};
					ps.processed = true;
					ps.response = true;
					insightMap.put(output.getInsightId(), output);
				} catch(Exception ex) {
					classLogger.error(Constants.STACKTRACE, ex);
					//ex.printStackTrace();
					//System.err.println("Method.. " + ps.methodName);
					ps.ex = ExceptionUtils.getStackTrace(ex);						
					//TCPChromeDriverUtility.quit("stop");
				}
				return ps;
			}
			else if(ps.operation == PayloadStruct.OPERATION.REACTOR)
			{
				try {
					Insight insight = insightMap.get(ps.insightId);
					// no need for another thread
					// you are already in a thread
					String reactorName = ps.objId;
					ps.response = true;
					
					// get the project
					// Project serves no purpose other than just giving me the reactor
					
					//TODO: on tomcat side, when context changes needs to be told
					//TODO: on tomcat side, when context changes needs to be told
					//TODO: on tomcat side, when context changes needs to be told
					//TODO: on tomcat side, when context changes needs to be told

					// 1) we need to check insight context project
					// 2) then we need to check the project the insight is saved in
					// note for 2 - this can be null
					
					IReactor reactor = null;
					String contextProjectId = insight.getContextProjectId();
					if(contextProjectId != null) {
						reactor = getProjectReactor(contextProjectId, insight.getContextProjectName(), reactorName);
					}
					if(reactor == null && insight.getProjectId() != null) {
						reactor = getProjectReactor(insight.getProjectId(), insight.getProjectName(), reactorName);
					}
					if(reactor == null) {
						throw new NullPointerException("Could not find reactor with name " + reactorName);
					}
					reactor.setInsight(insight);
					reactor.setNounStore((NounStore)ps.payload[0]);
					classLogger.info("Set the nounstore on reactor");
					
					// execute
					reactor.In();
					NounMetadata nmd = reactor.execute();
					classLogger.info("Execution of reactor complete");
					// return the response
					ps.payload = new Object[] {nmd};
					ps.payloadClasses = new Class[] {NounMetadata.class};
				} catch(Exception ex) {
					classLogger.error(Constants.STACKTRACE, ex);
					//ex.printStackTrace();
					//System.err.println("Method.. " + ps.methodName);
					ps.ex = ExceptionUtils.getStackTrace(ex);						
					//TCPChromeDriverUtility.quit("stop");
				}
				return ps;
			}
			else if(ps.operation == PayloadStruct.OPERATION.PROJECT)
			{
				// make a method call
				try {
					Project project = projectMap.get(ps.projectId);
					if(project == null)
						project = makeProject(ps.projectId, ps.projectName);
					
					if(project != null)
					{
						Method method = findProjectMethod(project, ps.methodName, ps.payloadClasses);
				    	Object retObject = null;					    	
						retObject = method.invoke(project, ps.payload);
						ps.processed = true;
						ps.response = true;
					}
					ps.payload = new Object [] {"method "+ ps.methodName + " execution complete"};
					ps.payloadClasses = new Class [] {String.class};
				} catch(Exception ex) {
					classLogger.error(Constants.STACKTRACE, ex);
					//ex.printStackTrace();
					//System.err.println("Method.. " + ps.methodName);
					ps.ex = ExceptionUtils.getStackTrace(ex);						
					//TCPChromeDriverUtility.quit("stop");
				}
				return ps;
			}else if(ps.operation == PayloadStruct.OPERATION.CMD)
			{
				// make a method call
				try {
					if(ps.methodName.equalsIgnoreCase("constructor")) {
						String mountName = ""+ ps.payload[0];
						String dir = "" + ps.payload[1];
						if(!cmdMap.containsKey(mountName + "__" + dir))
						{
							CmdExecUtil cmd = new CmdExecUtil(mountName, dir, null);
							cmdMap.put(mountName + "__" + dir, cmd);
						}
						ps.payload = new Object [] {"constructor execution complete"};
						ps.payloadClasses = new Class [] {String.class};
					} else {
						CmdExecUtil thisCmd = cmdMap.get(ps.insightId);
						if(thisCmd != null)
						{
							String output = thisCmd.executeCommand(""+ps.payload[0]);
							ps.processed = true;
							ps.response = true;
							ps.payload = new Object[] {output};
						}
					}
				} catch(Exception ex) {
					classLogger.error(Constants.STACKTRACE, ex);
					ps.ex = ExceptionUtils.getStackTrace(ex);						
					//TCPChromeDriverUtility.quit("stop");
				}

				return ps;
			}
		} catch(Exception ex) {
			ex.printStackTrace();
			classLogger.error(Constants.STACKTRACE, ex);
			ps.ex = ex.getMessage();
		}
		return null;
	}

	/**
	 * 
	 * @param projectId
	 * @param projectName
	 * @param reactorName
	 * @return
	 */
	private IReactor getProjectReactor(String projectId, String projectName, String reactorName) {
		Project project = null;
		if(projectMap.containsKey(projectId)) {
			project = (Project) projectMap.get(projectId);
		} else {
			project = makeProject(projectId, projectName);
		}						
		// I dont know if I can do this or I have to use that jar class loader
		IReactor reactor = project.getReactor(reactorName, null);
		return reactor;
	}
	
	private Project makeProject(String projectId, String projectName)
	{
		Project project = new Project();
		project.setProjectId(projectId);
		project.setProjectName(projectName);
		// dont give me a wrapper.. give me the real reactor
		projectMap.put(projectId, project);
		String projectSock = projectId + "__SOCKET";
		DIHelper.getInstance().setProjectProperty(projectSock, project);
		
		return project;
	}
	
	public PayloadStruct writeResponse(PayloadStruct ps)
	{
		byte [] psBytes = null;
		// if this is the response
		// all set
		// package the bytes and send the response
		if(!ps.response || ps.epoc == null)
		{
			ps.epoc = "ss" + curEpoc;
			curEpoc++;
			outgoing.put(ps.epoc, ps);
		}
		
		try 
		{
			psBytes = FstUtil.packBytes(ps);
		}
		catch(Exception ex)
		{ 
			// dont choke this thread
			classLogger.error(Constants.STACKTRACE, ex);
			if(psBytes == null)
			{
				// hmm we are in the non serializable land
				// let us try it this way now
				ps.payload = new String[] {"Output is not serializable. Forcing stringify <" + ps.payload[0] + ">"};
				psBytes = FstUtil.packBytes(ps);
			}
		}
		
		// send it
		//System.out.println("  Sending bytes " + psBytes.length + " >> " + ps.methodName + "  " + ps.epoc + " >> ");
		classLogger.info("  Sending bytes " + psBytes.length + " >> " + ps.methodName + "  " + ps.epoc + " >> " );
		try
		{
			os.write(psBytes);
			// remove from the epoc queue
		}
		catch(Exception ex)
		{
			classLogger.error(Constants.STACKTRACE, ex);
		}

		// if this is what socket is sending 
		// i.e. response to an operation core semoss requested
		// job is done - clear it from the queues
		// incoming was the request, outgoing was the response
		if(ps.response) // clear from the current
		{
			// remove from unprocessed
			incoming.remove(ps.epoc);
			outgoing.remove(ps.epoc);
		}
		// if this is a request for core semoss
		// block the thread until we get response
		// notification happens in the run block see below
		// put the current structure into outgoing
		// block on that payload object
		// wait
		else // this is for interim operations
		{
			// put this into unprocessed
			// synchronize on the payload
			// and then wait
			//System.err.println(" Here in request " + ps);
			while(!incoming.containsKey(ps.epoc))
			{
				synchronized(ps)
				{
					try 
					{
						// wait to see if there is response
						classLogger.info("Going into wait for epoc " + ps.epoc);
						ps.wait(averageMillis); 
						// once response remove this from the outgoing queue
						// the main input is available on incoming
					} catch (InterruptedException e) 
					{
						e.printStackTrace();
						classLogger.error(Constants.STACKTRACE, e);
					}
				}
			}
			classLogger.info("Got re sponse for " + ps.epoc);
			// assumes we already got the response 
			outgoing.remove(ps.epoc);
			ps = incoming.remove(ps.epoc);
			return ps;

		}
		return null;
	}
	
	public void releaseAll()
	{
		// take all the unprocessed and remove all of it
		Iterator <String> keys = incoming.keySet().iterator();
		while(keys.hasNext())
		{
			String thisEpoc = keys.next();
			PayloadStruct ps = incoming.get(thisEpoc);
			
			if(ps != null)
			{
				String message = "Releasing this payload";
				if(ps.payload != null && ps.payload.length >= 1)
					message = message + ps.payload[0];
				ps.payload = new String[] {message};
				writeResponse(ps);
			}
		}
	}
	
	/**
	 * Delete the entire folder from insight cache and stop processes
	 */
	public void cleanUp() {
		try {
			if(!test) 
			{
				classLogger.info("Starting shutdown " );
				Iterator <String> envKeys = rtMap.keySet().iterator();
				while(envKeys.hasNext())
				{
					String env = envKeys.next();
					AbstractRJavaTranslator rt = rtMap.get(env);            	
					if(rt != null) {
						rt.endR();
					}
				}
				if(this.pt != null) {
					this.pt.killThread();
					processCommand("'logout now'"); // this should trigger it and kill it
				}
	
				// we should also close all the dbs that were opened
				String engines = DIHelper.getInstance().getEngineProperty(Constants.ENGINES) + "";
				if(engines != null)
				{
					String [] engineList = engines.split(";");
					for(int engineIndex = 0;engineIndex < engineList.length;engineIndex++)
					{
						IDatabaseEngine engine = Utility.getDatabase(engineList[engineIndex]);
						if(engine != null)
							engine.close();
					}
				}
			}
			// stop the classLogger
			LogManager.shutdown();
	
			// don't delete output log
			// do it later
			File outFile = new File(mainFolder + "/output.log");
			if(outFile.exists() && outFile.isFile()) {
				outFile.deleteOnExit();
			}
	
			try {
				FileUtils.deleteDirectory(new File(mainFolder));
			} catch (IOException ignore) {
				
			}
		} catch(Exception | Error e) {
			//ignore
		}
		
		// exit out
		System.exit(1);
	}
	
    // process the command
    public Object processCommand(String command)
    {
    	// this should be basically be just straight up runscript
    	classLogger.info("Running the new version");
		this.pt.command = new String[] {command};
		Object monitor = this.pt.getMonitor();
		Object response = null;
		synchronized(monitor) {
			try {
				monitor.notify();
				monitor.wait(4000); // need a better way.. what happens if it takes more than 4000 seconds ?
				response = this.pt.response.get(command);
			} catch (Exception ex) {
				classLogger.debug(ex);
				response = ex.getMessage();
			}
		}
		return response;
    }
    
    public Method findRMethod(AbstractRJavaTranslator rt, String methodName, Class [] arguments)
    {
    	Method retMethod = null;
    	
    	// look for it in the child class if not parent class
    	// we can even cache this later
    	try {
			if(arguments != null)
			{
				try
				{
					retMethod = rt.getClass().getDeclaredMethod(methodName, arguments);
				}
				catch(Exception ex)
				{
					//classLogger.error(Constants.STACKTRACE, ex);
				}
				if(retMethod == null) {
					retMethod = rt.getClass().getSuperclass().getDeclaredMethod(methodName, arguments);
				}
			}
			else
			{
				try
				{
					retMethod = rt.getClass().getDeclaredMethod(methodName);				
				}
				catch(Exception ex)
				{
					//classLogger.error(Constants.STACKTRACE, ex);	
				}
				if(retMethod == null) {
					retMethod = rt.getClass().getSuperclass().getDeclaredMethod(methodName, arguments);
				}
			}
			classLogger.info("Found the method " + retMethod);
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
			classLogger.error(Constants.STACKTRACE, e);
		} catch (SecurityException e) {
			e.printStackTrace();
			classLogger.error(Constants.STACKTRACE, e);
		}
    	return retMethod;
    }

    public Method findProjectMethod(Project rt, String methodName, Class [] arguments)
    {
    	Method retMethod = null;
    	
    	// look for it in the child class if not parent class
    	// we can even cache this later
    	try {
			if(arguments != null)
			{
				try
				{
					retMethod = rt.getClass().getDeclaredMethod(methodName, arguments);
				}
				catch(Exception ex)
				{
					//classLogger.error(Constants.STACKTRACE, ex);	
				}
				if(retMethod == null) {
					retMethod = rt.getClass().getSuperclass().getDeclaredMethod(methodName, arguments);
				}
				
			}
			else
			{
				try
				{
					retMethod = rt.getClass().getDeclaredMethod(methodName);				
				}
				catch(Exception ex)
				{
					//classLogger.error(Constants.STACKTRACE, ex);	
				}
				if(retMethod == null) {
					retMethod = rt.getClass().getSuperclass().getDeclaredMethod(methodName, arguments);
				}
			}
			classLogger.info("Found the method " + retMethod);
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
			classLogger.error(Constants.STACKTRACE, e);
		} catch (SecurityException e) {
			e.printStackTrace();
			classLogger.error(Constants.STACKTRACE, e);
		}
    	return retMethod;
    }

    
    public Method findPyMethod(String methodName, Class [] arguments)
    {
    	Method retMethod = null;
    	
    	try {
			if(arguments != null)
			{
				try
				{
					retMethod = pyt.getClass().getSuperclass().getDeclaredMethod(methodName, arguments);
				}
				catch(Exception ex)
				{
					//classLogger.error(Constants.STACKTRACE, ex);
				}
				if(retMethod == null) {
					retMethod = pyt.getClass().getDeclaredMethod(methodName, arguments);
				}
			}
			else
			{
				try
				{
					retMethod = pyt.getClass().getSuperclass().getDeclaredMethod(methodName);				
				}
				catch(Exception ex)
				{
					//classLogger.error(Constants.STACKTRACE, ex);
				}
				if(retMethod == null) {
					retMethod = pyt.getClass().getDeclaredMethod(methodName);
				}
			}
			//classLogger.info("Found the method " + retMethod);
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
			classLogger.error(Constants.STACKTRACE, e);
		} catch (SecurityException e) {
			e.printStackTrace();
			classLogger.error(Constants.STACKTRACE, e);
		}
    	return retMethod;
    }

    public Method findChromeMethod(String methodName, Class [] arguments)
    {
    	Method retMethod = null;
    	
    	try {
			if(arguments != null)
			{
				try
				{
					retMethod = TCPChromeDriverUtility.class.getDeclaredMethod(methodName, arguments);
				}
				catch(Exception ex)
				{
					//classLogger.error(Constants.STACKTRACE, ex);
				}
			}
			else
			{
				try
				{
					retMethod = TCPChromeDriverUtility.class.getDeclaredMethod(methodName);				
				}
				catch(Exception ex)
				{
					//classLogger.error(Constants.STACKTRACE, ex);	
				}
			}
			classLogger.info("Found the method " + retMethod);
		} catch (SecurityException e) {
			e.printStackTrace();
			classLogger.error(Constants.STACKTRACE, e);
		}
    	return retMethod;
    }

    
    public Object runMethodR(AbstractRJavaTranslator rt2, Method method, Object [] arguments) throws Exception
    {
    	
    	Object retObject = null;
    	
		retObject = method.invoke(rt2, arguments);
    	
    	return retObject;
    }

    public Object runMethodPy(Method method, Object [] arguments) throws Exception
    {
    	Object retObject = null;
    	
		retObject = method.invoke(pyt, arguments);
    	
    	return retObject;
    }
    

    public Object runMethodChrome(Method method, Object [] arguments) throws Exception
    {
    	Object retObject = null;
    	
		retObject = method.invoke(TCPChromeDriverUtility.class, arguments);
    	
    	return retObject;
    }

    private AbstractRJavaTranslator getTranslator(String env)
    {
    	if(!rtMap.containsKey(env))
    	{
    		boolean JRI = DIHelper.getInstance().getProperty(Constants.R_CONNECTION_JRI) == null || DIHelper.getInstance().getProperty(Constants.R_CONNECTION_JRI).equalsIgnoreCase("true");
    		AbstractRJavaTranslator arjt = null;
    		if(JRI)
    		{
	    		arjt = new RJavaJriTranslator();
	    		arjt.setLogger(classLogger);
	    		arjt.startR();
	    		arjt.initREnv(env);	
    		}
    		else // try doing rserve
    		{
    			arjt = new RJavaRserveTranslator();
    			if(retCon == null)
    			{
    				arjt.setLogger(classLogger);
    				arjt.startR();
    				this.retCon = ((RJavaRserveTranslator)arjt).getConnection();    				
    			}
    			else
    			{
					arjt.setLogger(classLogger);
					arjt.setConnection(retCon);
					arjt.initREnv(env);    				
    			}
    		}
    		rtMap.put(env, arjt);
    	}
    	return rtMap.get(env);
    }

	public void getPyTranslator()
	{
		if(this.pt== null)
		{
			pt = new PyExecutorThread();
			//pt.getJep();
			pt.start();
			
			while(!pt.isReady())
			{
				try {
					// sleep until we get the py
					Thread.sleep(200);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			classLogger.info("PyThread Started");
			setPyExecutorThread(this.pt);
//			System.err.println("Got the py thread");
		}
	}	

	public void setOutputStream(OutputStream os)
	{
		this.os = os;
	}
	
	public void setInputStream(InputStream is)
	{
		this.is = is;
	}
	
	public void setServerSocket(ServerSocket socket)
	{
		this.socket = socket;
	}
	
	@Override
	public void run()
	{
		// there are 2 types of interactions
		// #1 SEMOSS Core sends a request and this responds - In this case the packet comes with response = false to say this is a request
		// #2 This asks SEMOSS core to perform an operation like database insert or update etc. 

		while(!done)
		{
			try
			{
				int bytesToRead = offset;
				if(lenBytes != null && lenBytesReadSoFar == lenBytes.length) // only get in here if you have read everything
				{
					bytesToRead  = ByteBuffer.wrap(lenBytes).getInt();
					if(curBytes == null)
						curBytes = new byte[bytesToRead]; // block it

					int bytesRead = is.read(curBytes, bytesReadSoFar, (curBytes.length - bytesReadSoFar)); // block
					bytesReadSoFar = bytesReadSoFar + bytesRead;
					
					if(bytesReadSoFar == curBytes.length) // we got what we need.. let us go
					{
						Object retObject = FstUtil.deserialize(curBytes);
						
						// need something here which basically tries to see if this is a request or a response	
						// #1 - This is a request for socket - handle it
						if(!((PayloadStruct)retObject).response) // this is a request that is coming here
						{
							lenBytes = null;
							curBytes = null;
							bytesReadSoFar = 0;
							lenBytesReadSoFar = 0;

							if(blocking)
							{
								PayloadStruct output = getFinalOutput((PayloadStruct)retObject);
								writeResponse(output);

							}
							
							else
							{
								WorkerThread wt = new WorkerThread(this, (PayloadStruct)retObject);
								Thread t = new Thread(wt);
								t.start();
							}
						}
						// #2 - Response to an operation being performed by core semoss
						else
						{
							// this is a response to the request that just came in
							// synchronize on the ps and then notify
							PayloadStruct responseStruct = (PayloadStruct)retObject;
							classLogger.info("Received payload with epoc "+ responseStruct.epoc);
							PayloadStruct requestStruct = outgoing.get(responseStruct.epoc);
							classLogger.info("Have response with epoc " + outgoing.containsKey(responseStruct.epoc));
							incoming.put(responseStruct.epoc, responseStruct);
							if(requestStruct != null)
							{
								synchronized(requestStruct)
								{
									requestStruct.notifyAll(); // this will give the thread back what it was looking for
								}
							}
							lenBytes = null;
							curBytes = null;
							bytesReadSoFar = 0;
							lenBytesReadSoFar = 0;
						}
					}
				}
				else
				{
					if(lenBytes == null)
						lenBytes = new byte[bytesToRead]; // block it
					int bytesRead = is.read(lenBytes, lenBytesReadSoFar, (lenBytes.length - lenBytesReadSoFar)); // block
					lenBytesReadSoFar = lenBytesReadSoFar + bytesRead;
				}				
			} catch (IOException e) {
				e.printStackTrace();
				classLogger.error(Constants.STACKTRACE, e);
//				System.err.println("Client socket has been closed !");
				synchronized(server.crash)
				{
					try {
						// ask it to listen again
						this.done = true;
						server.crash.notify();
					} catch (Exception e1) {
						e1.printStackTrace();
						classLogger.error(Constants.STACKTRACE, e1);
					}
				}
				// dont quit.. work hard
				if(!SocketServer.isMulti()) {
					System.exit(1);
				}
			}
		}
	}
	
	public PayloadStruct getPayloadForEpoc(String epoc)
	{
		return incoming.get(epoc);
	}

}