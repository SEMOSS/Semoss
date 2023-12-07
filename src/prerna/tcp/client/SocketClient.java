package prerna.tcp.client;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import prerna.auth.User;
import prerna.om.Insight;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.tcp.PayloadStruct;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.FstUtil;
import prerna.util.Settings;
import prerna.util.Utility;

public class SocketClient implements Runnable, Closeable {
	
	private static final Logger classLogger = LogManager.getLogger(SocketClient.class);
	
    private String HOST = null;
    private int PORT = -1;
    private boolean SSL = false;
    
    Map requestMap = new HashMap();
    Map responseMap = new HashMap();
    private boolean ready = false;
    private boolean connected = false;
    private AtomicInteger count = new AtomicInteger(0);
    private long averageMillis = 200;
    private boolean killall = false; // use this if the server is dead or it has crashed
    private User user;
	
    private Socket clientSocket = null;
	InputStream is = null;
	OutputStream os = null;
	SocketClientHandler sch = new SocketClientHandler();
	Map <String, Insight> insightMap = new HashMap<String, Insight>();

	/**
	 * 
	 * @param HOST
	 * @param PORT
	 * @param SSL
	 */
    public void connect(final String HOST, final int PORT, final boolean SSL) {
    	this.HOST = HOST;
    	this.PORT = PORT;
    	this.SSL = SSL;
    }
    
    @Override
    public void close() {
    	if(this.requestMap != null) {
    		this.requestMap.clear();
    	}
    	closeStream(this.os);
    	closeStream(this.is);
    	closeStream(this.clientSocket);
    	this.connected = false;
    	this.killall = true;
    }

    @Override
    public void run()	
    {
        // Configure SSL.git
    	int attempt = 1;
    	int SLEEP_TIME = 800;
    	if(DIHelper.getInstance().getProperty("SLEEP_TIME") != null) {
    		SLEEP_TIME = Integer.parseInt(DIHelper.getInstance().getProperty("SLEEP_TIME"));
    	}
    	
    	classLogger.info("Trying with the sleep time of " + SLEEP_TIME);
    	while(!connected && attempt < 6) // I do an attempt here too hmm.. 
    	{
	    	try
	    	{
		        final SslContext sslCtx;
		        if (SSL) {
		            sslCtx = SslContextBuilder.forClient()
		                .trustManager(InsecureTrustManagerFactory.INSTANCE).build();
		        } else {
		            sslCtx = null;
		        }
		
		        // Configure the client.
				boolean blocking = DIHelper.getInstance().getProperty(Settings.BLOCKING) != null && DIHelper.getInstance().getProperty(Settings.BLOCKING).equalsIgnoreCase("true");
		        	
	    		clientSocket =  new Socket(this.HOST, this.PORT);
	    		
	    		// pick input and output stream and start the threads
	    		this.is = clientSocket.getInputStream();
	    		this.os = clientSocket.getOutputStream();
	    		sch.setClient(this);
	    		sch.setInputStream(this.is);
	    		
	    		// start this thread
	    		Thread readerThread = new Thread(sch);
	    		readerThread.start();
	    		
	            classLogger.info("CLIENT Connection complete !!!!!!!");
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
	    		classLogger.info("Attempting Number " + attempt);
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
    	
    	if(attempt > 6) {
            classLogger.info("CLIENT Connection Failed !!!!!!!");
            ready = true; // come out of the loop
            synchronized(this)
            {
            	this.notifyAll();
            }
    	}
    }	
    
    public Object executeCommand(PayloadStruct ps)
    {
    	if(killall) {
        	throw new SemossPixelException("Analytic engine is no longer available. This happened because you exceeded the memory limits provided or performed an illegal operation. Please relook at your recipe");
    	}
    	
    	if(!connected) {
        	throw new SemossPixelException("Your micro-process is not available. Please logout and try again. !");
    	}
    	
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
    		if(!ps.response) {
    			requestMap.put(id, ps);
    		}
    		classLogger.info("Outgoing epoc " + ps.epoc);
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
						if(pollNum < 10) {
							ps.wait(averageMillis);
						} else { //if(ps.longRunning) // this is to make sure the kill all is being checked
							ps.wait(); // wait eternally - we dont know how long some of the load operations would take besides, I am not sure if the null gets us anything
						}
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
					classLogger.info("Timed out for epoc " + ps.epoc + " " + ps.methodName);
					
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
    	try {
    		os.write(psBytes);
    	} catch(IOException ex) {
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

    
    public void stopPyServe(String dir) {
    	if(isConnected()) {
	    	PayloadStruct ps = new PayloadStruct();
	    	ps.methodName = "CLOSE_ALL_LOGOUT<o>";
	    	ps.payload = new String[] { "CLOSE_ALL_LOGOUT<o>"};
	    	writePayload(ps);
    	}
    	
		CleanerThread t = new CleanerThread(dir);
		t.start();
    }

    /**
     * 
     */
    public void crash() {
    	// this happens when the client has completely crashed
    	// make the connected to be false
    	// take everything that is waiting on it
    	// go through request map and start pushing
    	try {
	    	for(Object k : this.requestMap.keySet()) {
	    		PayloadStruct ps = (PayloadStruct) this.requestMap.get(k);
	    		classLogger.debug("Releasing <" + k + "> <" + ps.methodName + ">");
	    		ps.ex = "Server has crashed. This happened because you exceeded the memory limits provided or performed an illegal operation. Please relook at your recipe";
	    		synchronized(ps) {
	    			ps.notifyAll();
	    		}
	    	}
    	} catch(Exception e) {
    		classLogger.error(Constants.STACKTRACE, e);
    	}
    	
    	this.close();
    	throw new SemossPixelException("Analytic engine is no longer available. This happened because you exceeded the memory limits provided or performed an illegal operation. Please relook at your recipe");
    }
    
    /**
     * 
     * @param closeThis
     */
    private void closeStream(Closeable closeThis) {
    	if(closeThis != null) {
	    	try {
				closeThis.close();
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
    	}
    }
    
    /**
     * 
     * @param user
     */
    public void setUser(User user) {
    	this.user = user;
    }

    /**
     * 
     * @return
     */
    public User getUser() {
    	return this.user;
    }
    
    /**
     * 
     * @param connected
     */
    public void setConnected(boolean connected) {
		this.connected = connected;
	}
    
    /**
     * 
     * @return
     */
    public boolean isConnected() {
    	return this.connected;
    }
    
    /**
     * 
     * @return
     */
    public boolean isReady() {
    	return this.ready;
    }
    

    public void addInsight2Insight(String insightId, Insight insight)
    {
    	insightMap.put(insightId, insight);
    }

    
}
