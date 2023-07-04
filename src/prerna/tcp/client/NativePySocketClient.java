package prerna.tcp.client;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;

import io.netty.channel.ChannelFuture;
import prerna.auth.User;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.tcp.PayloadStruct;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.FstUtil;
import prerna.util.Utility;

public class NativePySocketClient extends SocketClient implements Runnable  {
	
	private static final String CLASS_NAME = NativePySocketClient.class.getName();
	private static final Logger logger = LogManager.getLogger(CLASS_NAME);
	
    private String HOST = null;
    private int PORT = -1;
    private boolean ssl = false;
    Map requestMap = new HashMap();
    Map responseMap = new HashMap();
    private boolean ready = false;
    private boolean connected = false;
    private AtomicInteger count = new AtomicInteger(0);
    private long averageMillis = 200;
    private boolean killall = false; // use this if the server is dead or it has crashed
    private User user;
	
    private Socket clientSocket = null;
	//InputStream is = null;
	//OutputStream os = null;
	SocketClientHandler sch = new SocketClientHandler();
	Gson gson = new Gson();

	
    public void connect(String HOST, int PORT, boolean SSL)
    {
    	this.HOST = HOST;
    	this.PORT = PORT;
    	this.ssl = SSL;
    }
    
    public ChannelFuture disconnect()
    {
    	return null;
    }

    public void run()	
    {
    	// there is 2 portions to the run
    	// one is before connect
    	// one is after. The reason this is done is to avoid an extra handler for information
    	
        // Configure SSL.git
    	if(!connected && !killall)
    	{
	    	int attempt = 1;
	    	int SLEEP_TIME = 800;
	    	if(DIHelper.getInstance().getProperty("SLEEP_TIME") != null) {
	    		SLEEP_TIME = Integer.parseInt(DIHelper.getInstance().getProperty("SLEEP_TIME"));
	    	}
	    	
	    	logger.info("Trying with the sleep time of " + SLEEP_TIME);
	    	while(!connected && attempt < 6) // I do an attempt here too hmm.. 
	    	{
		    	try
		    	{
		    		clientSocket =  new Socket(this.HOST, this.PORT);
		    		
		    		// pick input and output stream and start the threads
		    		this.is = clientSocket.getInputStream();
		    		this.os = clientSocket.getOutputStream();
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
	    	
	    	if(attempt > 6) {
	            logger.info("CLIENT Connection Failed !!!!!!!");
	            killall = true;
	    	}
    	}    	
    	
    	// this is the read portion
    	if(connected)
    	{
    		StringBuffer outputAssimilator = new StringBuffer("");
    		while (!killall) 
    		{
    			try {
    				byte[] length = new byte[4];
    				is.read(length);

    				int size = ByteBuffer.wrap(length).getInt();
    				System.err.println("Incoming data is of size " + size);
    				
    				if(size > 0)
    				{
	    				byte[] msg = new byte[size];
	    				int size_read = 0;
	    				while(size_read < size)
	    				{
	    					byte [] newMsg = new byte[size];
	    					int cur_size = is.read(newMsg);
	    					System.arraycopy(newMsg, 0, msg, size_read, cur_size);
	    					size_read = size_read + cur_size;
	    					System.out.println("incoming size " + size + "  read size.. " + size_read);
	    				}
	
	    				String message = new String(msg);
	    				System.err.print(message);
	    				PayloadStruct ps = gson.fromJson(message, PayloadStruct.class);
	    				
	    				if(ps.operation == ps.operation.STDOUT && ps.payload != null)
	    				{
	    					logger.info(ps.payload[0]);
	    					outputAssimilator.append(ps.payload[0]);
	    				}
	       				// need some way to say this is the output from the actual python vs. something that is a logger
	    				// this is done through interim and operations
	    				if(ps.interim && ps.response) // this is interim.. 
	    				{
	    					// need to return output here
	    					outputAssimilator.append(ps.payload[0]);
	    				}
	    				else if(ps.response)// interim is over
	    				{
	    					// we are going to force the output since that is they may have requested
	    					if(outputAssimilator.length() > 0 && ((String)ps.payload[0]).equalsIgnoreCase("NONE")) 
	    						ps.payload[0] = outputAssimilator;
	    					
	    					// try to convert it into a full object
	    					try
	    					{
		    					Object obj = gson.fromJson((String)ps.payload[0], Object.class);
		    					ps.payload[0] = obj;
	    					}catch(Exception ignored)
	    					{
	    						
	    					}
	    					
	    					System.out.println("FINAL OUTPUT <<<<<<<" + outputAssimilator + ">>>>>>>>>>>>");
	    					// re-initialize it
	    					outputAssimilator = new StringBuffer("");

	    					PayloadStruct lock = (PayloadStruct)requestMap.remove(ps.epoc);
	    					// put it in response
	    					responseMap.put(ps.epoc, ps);
	    					if(lock != null)
	    					{
	    						synchronized(lock)
	    						{
	    							lock.notifyAll();
	    						}
	    					}
	    				}
    				}
    				else
    				{
    					killall = true;
    					break;
    				}
    			} catch (Exception ex) {
    				ex.printStackTrace();
    				//killall = true;
    				//connected=false;
    				//break;
    			}
    		}
    		connected = false;
    		System.err.println("outside the run loop");
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
    	// nulling the classes so they dont screw up json
    	ps.payloadClasses = null;
    	try
    	{
    		String jsonPS = gson.toJson(ps);
    		byte [] psBytes = pack(jsonPS);
    		try {
    			os.write(psBytes);
    		} catch(IOException ex) {
    		ex.printStackTrace();
    		//crash();
    		}
    	}catch(Exception ex)
    	{
    		ex.printStackTrace();
    	}
    }
    
	public byte[] pack(String message) {
		byte[] psBytes = message.getBytes(StandardCharsets.UTF_8);

		// get the length
		int length = psBytes.length;

		System.err.println("Packing with length " + length);

		// make this into array
		byte[] lenBytes = ByteBuffer.allocate(4).putInt(length).array();

		// pack both of these
		byte[] finalByte = new byte[psBytes.length + lenBytes.length];

		for (int lenIndex = 0; lenIndex < lenBytes.length; lenIndex++)
			finalByte[lenIndex] = lenBytes[lenIndex];

		for (int lenIndex = 0; lenIndex < psBytes.length; lenIndex++)
			finalByte[lenIndex + lenBytes.length] = psBytes[lenIndex];

		return finalByte;

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
    	
		CleanerThread t = new CleanerThread(dir);
		t.start();
    }

    public void crash()
    {
    	// this happens when the client has completely crashed
    	// make the connected to be false
    	// take everything that is waiting on it
    	// go through request map and start pushing
    	for(Object k : this.requestMap.keySet()) {
    		PayloadStruct ps = (PayloadStruct) this.requestMap.get(k);
    		logger.debug("Releasing <" + k + "> <" + ps.methodName + ">");
    		ps.ex = "Server has crashed. This happened because you exceeded the memory limits provided or performed an illegal operation. Please relook at your recipe";
    		synchronized(ps) {
    			ps.notifyAll();
    		}
    	}
    	
    	this.requestMap.clear();
    	closeStream(this.os);
    	closeStream(this.is);
    	closeStream(this.clientSocket);
    	this.connected = false;
    	this.killall = true;
    	throw new SemossPixelException("Analytic engine is no longer available. This happened because you exceeded the memory limits provided or performed an illegal operation. Please relook at your recipe");
    }
    
    private void closeStream(Closeable closeThis) {
    	try {
			closeThis.close();
		} catch (IOException e) {
			logger.error(Constants.STACKTRACE, e);
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
    
}
