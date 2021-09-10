package prerna.tcp;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.Logger;
import org.openqa.selenium.chrome.ChromeDriver;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoopGroup;
import jep.JepException;
import prerna.ds.py.PyExecutorThread;
import prerna.ds.py.PyTranslator;
import prerna.sablecc2.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.reactor.frame.r.util.RJavaJriTranslator;
import prerna.util.FstUtil;
import prerna.util.TCPChromeDriverUtility;

public class BinaryServerHandler extends ChannelInboundHandlerAdapter {
	
	String inputSoFar = new String();
	
	public static final int NUM_ATTEMPTS = 3;
	
	String returnOutput = "<o>";
	String noOutput = "<e>";
	ByteBuf buf = null;
	
	PyExecutorThread pt = null;
	PyTranslator pyt = null;
	String mainFolder = null;
	boolean test = false;
	
	EventLoopGroup bossGroup = null;
	EventLoopGroup workerGroup = null;
	byte [] bytes = null;
	
	int totalBytes = -1;
	Object lock = new Object();
	Object singleThreadOperation = new Object();
	
	Map <String, PayloadStruct> unprocessed = new HashMap<String, PayloadStruct>();
	Map <String, Integer> attemptCount = new HashMap<String, Integer>();
	Map <String, AbstractRJavaTranslator> rtMap = new HashMap<String, AbstractRJavaTranslator>();
	
	public  Logger LOGGER = null;
	
	public void setMainFolder(String mainFolder) {
		this.mainFolder = mainFolder;
	}
	
	public void setLogger(Logger LOGGER) {
		this.LOGGER = LOGGER;
	}
	
	public void setPyExecutorThread(PyExecutorThread pt) {
		this.pt = pt;
		this.pyt = new PyTranslator();
		pyt.setPy(pt);;
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) {
			ByteBuf in = (ByteBuf)msg;
			
			if(buf == null) {
				//System.err.print("." + in.readableBytes());
				//synchronized(lock)
				{
					buf = in.copy();
				}
				if(totalBytes == -1)
				{
					getTotal();
				}
			}
			else		
			{
				// get the total bytes if there was
				// since this could be coming in from the other piece
				// need to convert to bytes
				if(totalBytes == -1) // possibility of coming from get readable bytes not having enough
				{
					getTotal();
				}

				synchronized(lock)
				{
					int curLen = buf.readableBytes() + 4;
					int inLen = in.readableBytes();
					int newLen = curLen + inLen;
					buf = buf.capacity(newLen);
					//System.err.println("New Buf capacity " + buf.capacity());
					// so this doesnt update the write index. which is why everything screws up
					// because readable bytes = writerIndex - reader index
					in.getBytes(0, buf, curLen, in.readableBytes());
					// manually set the writer index
					buf.writerIndex(newLen);
				}
			}
			in.release();	
    		//System.err.println(" Read " + "Readable byte " + buf.readableBytes() + "  index " + buf.readerIndex() + "  writer " + buf.writerIndex());
    		LOGGER.info(" Read " + "Readable byte " + buf.readableBytes() + "  index " + buf.readerIndex() + "  writer " + buf.writerIndex());
			// try to see if we have enough to trigger
			//System.err.println("Comparing.. " + buf.capacity() + "<<>>" + totalBytes);
			if(buf.readableBytes() >= totalBytes)
			{
				LOGGER.info("Sending for processing now  " + totalBytes);
				byte[]  data = new byte[totalBytes];
				buf.readBytes(data); 

				// start the payload processing
				PayloadStruct ps2 = null;
		    	try
		    	{
					ps2 = (PayloadStruct)FstUtil.deserialize(data);
					LOGGER.info("Serialized the payload ");
		    		if(ps2.methodName.equalsIgnoreCase("EMPTYEMPTYEMPTY") || ps2.methodName.equalsIgnoreCase("echo"))
		    		{
		    			//System.out.println("No response being written  >> ");
		    			LOGGER.info("No response being written  >> ");
		    		}
		    		else if(ps2 != null)
		    		{
			    		PayloadStruct ps = getFinalOutput(ctx, ps2);
			    		if(ps != null)
			    			ps2 = ps;
		    		}
		    		else if(ps2 == null) // huh ?
		    		{
		    			LOGGER.info("Not sending the ps - not empty but null");
		    		}
		    	} 
		    	catch (Exception ex)
		    	{
		    		// how does it even get here ?
		    		LOGGER.info("exception while sending " + ex);
		    		ex.printStackTrace();
		    	} 
		    	finally 
		    	{
	    			writeResponse(ctx, ps2);
		    	}
			}
	}
	
	// this is where the processing happens
	public PayloadStruct getFinalOutput(ChannelHandlerContext ctx, PayloadStruct ps)
	{
		{
			try
			{
				LOGGER.info("Starting to prcess " + totalBytes + "<>" + buf.readableBytes());
				
				//System.err.println("Received For Processing " + ps.methodName +  "  bytes : " + totalBytes + " Epoc " + ps.epoc);
				LOGGER.info("Received For Processing " + ps.methodName +  "  bytes : " + totalBytes + " Epoc " + ps.epoc);
				unprocessed.put(ps.epoc, ps);
				attemptCount.put(ps.epoc, 1);

				////System.err.println("Payload set to " + ps);
				if(ps.methodName.equalsIgnoreCase("EMPTYEMPTYEMPTY")) // trigger message ignore
					return ps;
				if(ps.methodName.equalsIgnoreCase("CLOSE_ALL_LOGOUT<o>")) // we are done kill everything
					cleanUp(ctx);

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
						if(output != null)
							LOGGER.info("Output is not null - PY");
						Object [] retObject = new Object[1];
						retObject[0] = output;
						ps.payload = retObject;
						ps.processed = true;
					}catch(Exception ex)
					{
						LOGGER.debug(ex);
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
			}catch(Exception ex)
			{
				ex.printStackTrace();
				ps.ex = ex.getMessage();
			}finally
			{
				// should this be in a lock
				// since it be parallel updating ?
				//synchronized(lock)
				{
					if(buf.readableBytes() > 0)
					{
						// need to see how many more bytes remain
						// do we need to do this
						// given it is a full block?
						//buf.discardReadBytes( );
						//System.err.println("whoa.. this is interesting.. ");
			    		//System.err.println("Post processing " + "Readable byte " + buf.readableBytes() + "  index " + buf.readerIndex() + "  writer " + buf.writerIndex());
			    		LOGGER.info("Post processing " + "Readable byte " + buf.readableBytes() + "  index " + buf.readerIndex() + "  writer " + buf.writerIndex());
						ByteBuf buf2 = buf.copy(buf.readerIndex(), buf.readableBytes());
						buf.release();
						buf = buf2;
			    		//System.err.println("Post processing - After Copy " + "Readable byte " + buf.readableBytes() + "  index " + buf.readerIndex() + "  writer " + buf.writerIndex());
			    		getTotal();
					   	if(buf.readableBytes() >= totalBytes)
					   	{
					   		//System.err.println("Last piece ? probably ?");
					   		LOGGER.info("This is possibly the offender ? " + totalBytes + " <> " + buf.readableBytes());
							byte[]  data2 = new byte[totalBytes];
							buf.readBytes(data2); 
							
							PayloadStruct ps2 = (PayloadStruct)FstUtil.deserialize(data2);
					   		return getFinalOutput(ctx, ps2);
					   	}
					}
					else
					{
						//
						buf.release();
						buf = null;
						totalBytes = -1;
						//buf = null;
					}
				}
				return ps;
			}
		}
	}
	
	private void getTotal()
	{
		if(buf.readableBytes() >= 4)
		{
			bytes = new byte[4];
			buf.readBytes(bytes);
		   	ByteBuffer wrapped = ByteBuffer.wrap(bytes); // big-endian by default
		   	totalBytes = wrapped.getInt(); // 1
		   	//System.out.println(" >> " + totalBytes + " <> " + buf.readableBytes());
		   	LOGGER.info(" >> " + totalBytes + " <> " + buf.readableBytes());
		}
		else totalBytes = -1;
	}
	

	
	private void writeResponse(ChannelHandlerContext ctx, PayloadStruct ps)
	{
		byte [] psBytes = FstUtil.serialize(ps);
		//System.out.println("  Sending bytes " + psBytes.length + " >> " + ps.methodName + "  " + ps.epoc + " >> ");
		LOGGER.info("  Sending bytes " + psBytes.length + " >> " + ps.methodName + "  " + ps.epoc + " >> ");
		// remove it
		unprocessed.remove(ps.epoc);
		attemptCount.remove(ps.epoc);
		//psBytes = "abcd".getBytes();
		//System.err.println("Writing payload " + psBytes.length);
		
		ByteBuf buff = Unpooled.buffer(psBytes.length);
		buff.writeBytes(psBytes);
		
		ctx.write(buff);
		ctx.flush();
		
		// try to process it
		//processUnProcessed(ctx);
		inputSoFar = "";
		LOGGER.info("Result Flushed " + ps.methodName);
	}
	
	private void processUnProcessed(ChannelHandlerContext ctx)
	{
		Iterator <String> procKeys = unprocessed.keySet().iterator();
		while(procKeys.hasNext())
		{
			String thisKey = procKeys.next();
			LOGGER.info("Performing secondary processing.. " + thisKey);
			PayloadStruct ps = (PayloadStruct) unprocessed.get(thisKey);
			int attempt = attemptCount.get(thisKey);
			
			//if(attempt > NUM_ATTEMPTS)
			{
				//attemptCount.remove(thisKey);
				writeResponse(ctx, ps); // no luck sorry
			}
			/*
			else
			{
				attemptCount.put(thisKey, attempt++);
				PayloadStruct newPS = getFinalOutput(ps);
				if(newPS != null)
					writeResponse(ctx, newPS);
			}
			*/
		}
	}

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
    	//processData(ctx);
    	//PayloadStruct ps = getFinalOutput();
    	//writeResponse(ctx, ps);
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Close the connection when an exception is raised.
        //cause.printStackTrace();
    	// connection has been closed by the host
    	// may be tomcat
    	//System.err.println("Exception ..  " + cause);
    	if(cause instanceof SocketException 
    			|| cause instanceof IOException 
    			|| cause instanceof OutOfMemoryError 
    			|| ((cause instanceof JepException) && ((JepException)cause).getCause() instanceof OutOfMemoryError )
    			|| ((cause instanceof JepException) && ((JepException)cause).getCause() instanceof VirtualMachineError )    		   
    			)
    		cleanUp(ctx);
    	
    }
    
    public void cleanUp(ChannelHandlerContext ctx)
    {
    	if(!test)
    	{
    		LOGGER.info("Starting shutdown " );
    		ctx.close(); // close the context and take out everything else
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

    	ctx.channel().close();
    	ctx.channel().parent().close();

    	bossGroup.shutdownGracefully();
    	workerGroup.shutdownGracefully();

    	// dont delete output log
    	// do it later
    	File file = new File(mainFolder + "/output.log");
    	file.delete();

    	try {
    		FileUtils.deleteDirectory(new File(mainFolder));
    	} catch (IOException ignore) {
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
    
    public void setBossGroup(EventLoopGroup bossGroup)
    {
    	this.bossGroup = bossGroup;
    }
    
    public void setWorkerGroup(EventLoopGroup workerGroup)
    {
    	this.workerGroup = workerGroup;
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
	

}