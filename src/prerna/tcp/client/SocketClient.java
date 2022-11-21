package prerna.tcp.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Iterator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.channel.ChannelFuture;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import prerna.auth.User;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.tcp.PayloadStruct;
import prerna.util.DIHelper;
import prerna.util.FstUtil;
import prerna.util.Settings;
import prerna.util.Utility;

public class SocketClient extends Client implements Runnable {

	private static final Logger logger = LogManager.getLogger(SocketClient.class);
	boolean done = false;
	InputStream is = null;
	OutputStream os = null;
	SocketClientHandler sch = new SocketClientHandler();
	User user = null; // the main user for this socket client
	
	boolean killall = false; // use this if the server is dead or it has crashed
    
    public void connect(String HOST, int PORT, boolean SSL)
    {
    	this.HOST = HOST;
    	this.PORT = PORT;
    	this.ssl = SSL;
    }
    
    public ChannelFuture disconnect()
    {
    	done = true;
    	return null;
    }

    public void run()	
    {
        // Configure SSL.git
    	int attempt = 1;
    	int SLEEP_TIME = 800;
    	if(DIHelper.getInstance().getProperty("SLEEP_TIME") != null)
    		SLEEP_TIME = Integer.parseInt(DIHelper.getInstance().getProperty("SLEEP_TIME"));
    	
    	logger.info("Trying with the sleep time of " + SLEEP_TIME);
    	while(!connected && attempt < 6) // I do an attempt here too hmm.. 
    	{
	    	try
	    	{
		        final SslContext sslCtx;
		        if (ssl) {
		            sslCtx = SslContextBuilder.forClient()
		                .trustManager(InsecureTrustManagerFactory.INSTANCE).build();
		        } else {
		            sslCtx = null;
		        }
		
		        // Configure the client.
				boolean blocking = DIHelper.getInstance().getProperty(Settings.BLOCKING) != null && DIHelper.getInstance().getProperty(Settings.BLOCKING).equalsIgnoreCase("true");
		        	
	    		Socket clientSocket =  new Socket(this.HOST, this.PORT);
	    		
	    		// pick input and output stream and start the threads
	    		this.is = clientSocket.getInputStream();
	    		this.os = clientSocket.getOutputStream();
	    		sch.setClient(this);
	    		sch.setInputStream(is);
	    		
	    		// start this thread
	    		Thread readerThread = new Thread(sch);
	    		readerThread.start();
	    		
	            logger.info("CLIENT Connection complete !!!!!!!");
	            Thread.sleep(100); // sleep some before executing command
	            // prime it 
	            //logger.info("First command.. Prime" + executeCommand("2+2"));
	            connected = true;
	            ready = true;
	            killall = false;
	            synchronized(this)
	            {
	            	this.notifyAll();
	            }
	    	} catch(Exception ex) {
	    		attempt++;
	    		logger.info("Attempting Number " + attempt);
	    		// see if sleeping helps ?
	    		try {
	    			// sleeping only for 1 second here
	    			// but the py executor sleeps in 2 second increments
	    			Thread.sleep(attempt*SLEEP_TIME);
	    		} catch(Exception ex2) {
	    			// ignored
	    		}
	    	}
    	}
    	
    	if(attempt > 6)
            logger.info("CLIENT Connection Failed !!!!!!!");
    }	
    
    public boolean isReady() {
    	return this.ready;
    }
 
    public Object executeCommand(PayloadStruct ps)
    {
    	if(killall)
        	throw new SemossPixelException("Analytic engine is no longer available. This happened because you exceeded the memory limits provided or performed an illegal operation. Please relook at your recipe");
    	
    	if(!connected)
        	throw new SemossPixelException("Your micro-process is not available. Please logout and try again. !");

    	
    	int attempt = 0;
    	String id = ps.epoc;
    	if(!ps.response || id == null)
    	{
	    	id = "ps"+ count.getAndIncrement();
	    	ps.epoc = id;
    	}
    	ps.longRunning = true;
    	    	
    	synchronized(ps) // going back to single threaded .. earlier it was ps
    	{	
    		//if(ps.hasReturn)
    		// put it into request map
    		if(!ps.response)
    			requestMap.put(id, ps);

    		writePayload(ps);
	    	// send the message
			
    		// time to wait = average time * 10
    		// if this is a request wait for it
    		if(!ps.response) // this is a response to something the socket has asked
    		{
				int pollNum = 1; // 1 second
				while(!responseMap.containsKey(ps.epoc) && (pollNum <  10 || ps.longRunning) && !killall)
				{
					//logger.info("Checking to see if there was a response");
					try
					{
						if(pollNum < 10)
							ps.wait(averageMillis);
						else //if(ps.longRunning) // this is to make sure the kill all is being checked
							ps.wait(); // wait eternally - we dont know how long some of the load operations would take besides, I am not sure if the null gets us anything
						pollNum++;
					}catch (InterruptedException e) 
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					/*
					// trigger after 400 milliseconds
					if(pollNum == 2 && !ps.longRunning)
					{
						logger.info("Writing empty message " + ps.epoc);
						writeEmptyPayload();
					}
					*/
				}
				if(!responseMap.containsKey(ps.epoc) && ps.hasReturn)
				{
					logger.info("Timed out for epoc " + ps.epoc + " " + ps.methodName);
					
				}
    		}

			// after 10 seconds give up
			//printUnprocessed();
			return responseMap.remove(ps.epoc);
    	}
    }
    
    private void writePayload(PayloadStruct ps)
    {
    	byte [] psBytes = FstUtil.packBytes(ps);
    	try
    	{
    		os.write(psBytes);
    	}catch(IOException ex)
    	{
    		ex.printStackTrace();
    		crash();
    	}
    }
    
    private void writeEmptyPayload()
    {
    	PayloadStruct ps = new PayloadStruct();
    	ps.epoc=Utility.getRandomString(8);
    	ps.methodName = "EMPTYEMPTYEMPTY";
    	writePayload(ps);
    }
    

    public void writeReleaseAllPayload()
    {
    	PayloadStruct ps = new PayloadStruct();
    	ps.epoc=Utility.getRandomString(8);
    	ps.methodName = "RELEASE_ALL";
    	writePayload(ps);
    }

    
    public void stopPyServe(String dir)
    {
    	if(isConnected()) {
	    	PayloadStruct ps = new PayloadStruct();
	    	ps.methodName = "CLOSE_ALL_LOGOUT<o>";
	    	writePayload(ps);
    	}
    	// close the output stream
    	if(os != null) {
    		try {
    			os.close();
    		} catch(Exception e) {
    			e.printStackTrace();
    		}
    	}
    	
		CleanerThread t = new CleanerThread(dir);
		t.start();
    	//ctx.channel().close();
    	// Then close the parent channel (the one attached to the bind)
    	//ctx.channel().parent().close();
    }

    
    public void setResponse(String command, Object output)
    {
    	responseMap.put(command, output);
    }
    
    
    private void printUnprocessed()
    {
    	Iterator keys = requestMap.keySet().iterator();
    	logger.info("Unprocessed so far.. ");
    	while(keys.hasNext())
    	{
    		String thisKey = (String)keys.next();
    		System.err.print("<" + thisKey + ">" + "<" + ((PayloadStruct)requestMap.get(thisKey)).methodName);
    	}    	
    }
    
    public boolean isConnected()
    {
    	return this.connected;
    }
    
    public void crash()
    {
    	// this happens when the client has completely crashed
    	// make the connected to be false
    	// take everything that is waiting on it
    	// go through request map and start pushing
    	for(Object k : requestMap.keySet()) {
    		PayloadStruct ps = (PayloadStruct)requestMap.get(k);
    		ps.ex = "Server has crashed. This happened because you exceeded the memory limits provided or performed an illegal operation. Please relook at your recipe";
    		
    		synchronized(ps) {
    			ps.notifyAll();
    		}
    	}
    	requestMap.clear();
    	
    	this.connected = false;
    	killall = true;
    	status = "crashed";
    	
    	throw new SemossPixelException("Analytic engine is no longer available. This happened because you exceeded the memory limits provided or performed an illegal operation. Please relook at your recipe");
    }
    
    public void warmup()
    {
    	long totalMillis = 0;
    	for(int psCount = 0;psCount < 10;psCount++)
    	{
    		System.err.println("Warming " + psCount);
	    	PayloadStruct ps = new PayloadStruct();
	    	ps.methodName = "echo";
	    	ps.epoc = "echo" + psCount;
	    	Object [] time = new Object[1];
	    	time[0] =  LocalDateTime.now();  
	    	requestMap.put(ps.epoc, ps);
	    	
	    	synchronized(ps)
	    	{
	    		writePayload(ps);
	    		try 
	    		{
	    			lock.wait();
	    		}catch(Exception ex)
	    		{
	    			
	    		}
	    		
	    		// compare the time now
	    		PayloadStruct response = (PayloadStruct)responseMap.remove(ps.epoc);
	    		
	    		LocalDateTime sentTime = (LocalDateTime)response.payload[0];
	    		LocalDateTime receivedTime = LocalDateTime.now();
	    		
	    		totalMillis += Duration.between(sentTime, receivedTime).toMillis();	    		
	    		
	    	}
    	}    	
    	
    	averageMillis = totalMillis / 10;
    	
    	System.err.println("Average rountrip takes .. " + averageMillis);
    }
    
    public void setUser(User user)
    {
    	this.user = user;
    }

    public User getUser()
    {
    	return this.user;
    }
    
}
