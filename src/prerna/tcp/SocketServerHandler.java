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
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.Logger;
import org.openqa.selenium.chrome.ChromeDriver;

import prerna.ds.py.PyExecutorThread;
import prerna.ds.py.PyTranslator;
import prerna.sablecc2.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.reactor.frame.r.util.RJavaJriTranslator;
import prerna.util.FstUtil;
import prerna.util.TCPChromeDriverUtility;

public class SocketServerHandler implements Runnable
{
	
	int offset = 4;
	int totalBytes = 0;
	List<ByteBuffer> inputs = new Vector<ByteBuffer>();
	byte[] lenBytes = null;
	byte[] curBytes = null;
	int bytesReadSoFar = 0;
	int lenBytesReadSoFar = 0;
	boolean done = false;
	
	SocketServer server = null;
	
	
	Object processLock = new Object();
	
	OutputStream os = null;
	InputStream is = null;
	ServerSocket socket = null;
		
	public static final int NUM_ATTEMPTS = 3;
	
	PyExecutorThread pt = null;
	PyTranslator pyt = null;
	String mainFolder = null;
	boolean test = false;
			
	Map <String, AbstractRJavaTranslator> rtMap = new HashMap<String, AbstractRJavaTranslator>();
	
	public  Logger LOGGER = null;	
		
	public void setLogger(Logger LOGGER)
	{
		this.LOGGER = LOGGER;
	}
	
	public void setPyExecutorThread(PyExecutorThread pt)
	{
		this.pt = pt;
		this.pyt = new PyTranslator();
		pyt.setPy(pt);;
		
	}

	
	// this is where the processing happens
	public PayloadStruct getFinalOutput(PayloadStruct ps)
	{
		{
			try
			{				
				//System.err.println("Received For Processing " + ps.methodName +  "  bytes : " + totalBytes + " Epoc " + ps.epoc);
				//LOGGER.info("Received For Processing " + ps.methodName +  "  bytes : " + totalBytes + " Epoc " + ps.epoc);
				//unprocessed.put(ps.epoc, ps);
				//attemptCount.put(ps.epoc, 1);

				////System.err.println("Payload set to " + ps);
				if(ps.methodName.equalsIgnoreCase("EMPTYEMPTYEMPTY")) // trigger message ignore
					return ps;
				if(ps.methodName.equalsIgnoreCase("CLOSE_ALL_LOGOUT<o>")) // we are done kill everything
					cleanUp();

				if(ps.engine == PayloadStruct.ENGINE.R)
				{
					try
					{
						Method method = findRMethod(getTranslator(ps.env), ps.methodName, ps.payloadClasses);
						Object output = runMethodR(getTranslator(ps.env), method, ps.payload);
						if(output != null)
							LOGGER.info("Output is not null - R");
						Object [] retObject = new Object[1];
						retObject[0] = output;
						ps.payload = retObject;
						ps.processed = true;
					}catch(InvocationTargetException ex)
					{
						LOGGER.info(ex + ps.methodName);
						ex.printStackTrace();
						//System.err.println("Method.. " + ps.methodName);
						ps.ex = ExceptionUtils.getStackTrace(ex);						
					}catch(Exception ex )
					{
						LOGGER.info(ex + ps.methodName);
						ex.printStackTrace();
						//System.err.println("Method.. " + ps.methodName);
						ps.ex = ExceptionUtils.getStackTrace(ex);						
					}finally
					{
						return ps;
					}
				}
				if(ps.engine == PayloadStruct.ENGINE.PYTHON)
				{
					// get the py translator for the first time
					//getPyTranslator();
					try
					{
						Method method = findPyMethod(ps.methodName, ps.payloadClasses);
						Object output = runMethodPy(method, ps.payload);
						//if(output != null)
						//	LOGGER.info("Output is not null - PY");
						Object [] retObject = new Object[1];
						retObject[0] = output;
						ps.payload = retObject;
						ps.processed = true;
					}catch(Exception ex)
					{
						//LOGGER.debug(ex);
						ex.printStackTrace();
						//System.err.println("Method.. " + ps.methodName);
						ps.ex = ExceptionUtils.getStackTrace(ex);						
					}
				}
				else if(ps.engine == PayloadStruct.ENGINE.CHROME)
				{
					try
					{
						Method method = findChromeMethod(ps.methodName, ps.payloadClasses);
						Object output = runMethodChrome(method, ps.payload);
						if(output != null)
							LOGGER.info("Output is not null - CHROME");
						if(output instanceof ChromeDriver)
							output = new Object();
						if(output instanceof String)
							LOGGER.info("Output is >>>>>>>>>>>>>>>  " + output);
						Object [] retObject = new Object[1];
						retObject[0] = output;
						ps.payload = retObject;
						ps.processed = true;
					}catch(Exception ex)
					{
						LOGGER.debug(ex);
						//ex.printStackTrace();
						//System.err.println("Method.. " + ps.methodName);
						ps.ex = ExceptionUtils.getStackTrace(ex);						
						//TCPChromeDriverUtility.quit("stop");
					}
					
				}
				else if(ps.engine == PayloadStruct.ENGINE.ECHO)
				{
					try
					{
						Method method = findChromeMethod(ps.methodName, ps.payloadClasses);
						Object output = ps.payload[0];
						Object [] retObject = new Object[1];
						retObject[0] = output;
						ps.payload = retObject;
						ps.processed = true;
					}catch(Exception ex)
					{
						LOGGER.debug(ex);
						//ex.printStackTrace();
						//System.err.println("Method.. " + ps.methodName);
						ps.ex = ExceptionUtils.getStackTrace(ex);						
						//TCPChromeDriverUtility.quit("stop");
					}
				}

			}catch(Exception ex)
			{
				ex.printStackTrace();
				ps.ex = ex.getMessage();
			}finally
			{
			}
			return ps;
		}
	}	

	
	public void writeResponse(PayloadStruct ps)
	{
		byte [] psBytes = FstUtil.packBytes(ps);
		//System.out.println("  Sending bytes " + psBytes.length + " >> " + ps.methodName + "  " + ps.epoc + " >> ");
		LOGGER.info("  Sending bytes " + psBytes.length + " >> " + ps.methodName + "  " + ps.epoc + " >> ");

		// remove it
		//unprocessed.remove(ps.epoc);
		//attemptCount.remove(ps.epoc);
		//psBytes = "abcd".getBytes();
		//System.err.println("Writing payload " + psBytes.length);
		
		try
		{
			os.write(psBytes);
		}catch(Exception ex)
		{
			
		}
		// try to process it
		//processUnProcessed(ctx);
		//LOGGER.info("Result Flushed " + ps.methodName);
	}
    
    public void cleanUp()
    {
    	if(!test)
    	{
    		LOGGER.info("Starting shutdown " );
            Iterator <String> envKeys = rtMap.keySet().iterator();
            while(envKeys.hasNext())
            {
            	String env = envKeys.next();
            	AbstractRJavaTranslator rt = rtMap.get(env);            	
				if(rt != null)
					rt.endR();
            }
			if(this.pt != null)
			{
				this.pt.killThread();
				processCommand("'logout now'"); // this should trigger it and kill it
			}

          }
        // stop the logger
		//LogManager.shutdown();
		
		
		// dont delete output log
		// do it later
		File file = new File(mainFolder + "/output.log");
		file.deleteOnExit();
		
		try {
			FileUtils.deleteDirectory(new File(mainFolder));
		} catch (IOException ignore) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
		}			
		// exit out
		System.exit(1);
    }	
    
    // process the command
    public Object processCommand(String command)
    {
    	// this should be basically be just straight up runscript
    	LOGGER.info("Running the new version");
		this.pt.command = new String[] {command};
		Object monitor = this.pt.getMonitor();
		Object response = null;
		synchronized(monitor) {
			try {
				monitor.notify();
				monitor.wait(4000); // need a better way.. what happens if it takes more than 4000 seconds ?
				response = this.pt.response.get(command);
			} catch (Exception ex) {
				LOGGER.debug(ex);
				response = ex.getMessage();
			}
		}
		return response;
    }
    
    public void setTest(boolean test)
    {
    	this.test = test;
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
				}catch(Exception ex)
				{
					
				}
				if(retMethod == null)
					retMethod = rt.getClass().getSuperclass().getDeclaredMethod(methodName, arguments);
				
			}
			else
			{
				try
				{
					retMethod = rt.getClass().getDeclaredMethod(methodName);				
				}catch(Exception ex)
				{
					
				}
				if(retMethod == null)
					retMethod = rt.getClass().getSuperclass().getDeclaredMethod(methodName, arguments);
			}
			LOGGER.info("Found the method " + retMethod);
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
				}catch(Exception ex)
				{
					
				}
				if(retMethod == null)
					retMethod = pyt.getClass().getDeclaredMethod(methodName, arguments);
				
			}
			else
			{
				try
				{
					retMethod = pyt.getClass().getSuperclass().getDeclaredMethod(methodName);				
				}catch(Exception ex)
				{
					
				}
				if(retMethod == null)
					retMethod = pyt.getClass().getDeclaredMethod(methodName);
			}
			//LOGGER.info("Found the method " + retMethod);
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
				}catch(Exception ex)
				{
					
				}
			}
			else
			{
				try
				{
					retMethod = TCPChromeDriverUtility.class.getDeclaredMethod(methodName);				
				}catch(Exception ex)
				{
					
				}
			}
			LOGGER.info("Found the method " + retMethod);
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
    		AbstractRJavaTranslator arjt = new RJavaJriTranslator();
    		arjt.setLogger(LOGGER);
    		arjt.startR();
    		arjt.initREnv(env);
    		
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
			// make it blocking
			pt.run();
			
			
			while(!pt.isReady())
			{
				try {
					// sleep until we get the py
					Thread.sleep(200);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			
			LOGGER.info("PyThread Started");
			pyt = new PyTranslator();
			pyt.setPy(pt);
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
						PayloadStruct output = getFinalOutput((PayloadStruct)retObject);

						writeResponse(output);
						lenBytes = null;
						curBytes = null;
						bytesReadSoFar = 0;
						lenBytesReadSoFar = 0;
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
				// TODO Auto-generated catch block
				// something abruptly disconnected
				System.err.println("Client socket has been closed !");
				synchronized(server.crash)
				{
	
					try {
						// ask it to listen again
						this.done = true;
						server.crash.notify();
					} catch (Exception e1) {
						// TODO Auto-generated catch block
						//e1.printStackTrace();
					}
				}
				// dont quit.. work hard
				if(!this.server.multi)
					System.exit(1);
			}
		}
	}

	}