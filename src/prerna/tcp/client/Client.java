package prerna.tcp.client;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.oio.OioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.channel.socket.oio.OioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.tcp.PayloadStruct;
import prerna.util.DIHelper;
import prerna.util.FstUtil;
import prerna.util.Settings;

public class Client implements Runnable {

	private static final String CLASS_NAME = Client.class.getName();
	private static final Logger logger = LogManager.getLogger(CLASS_NAME);
	
	ChannelFuture f = null;
	EventLoopGroup group = null;
	public ChannelHandlerContext ctx = null;
    static final int SIZE = Integer.parseInt(System.getProperty("size", "256"));
    Object lock = new Object();
    Object response = null;
    String HOST = null;
    int PORT = -1;
    boolean ssl = false;
    Map requestMap = new HashMap();
    Map responseMap = new HashMap();
    boolean ready = false;
    boolean connected = false;
	AtomicInteger count = new AtomicInteger(0);
	long averageMillis = 200;
	boolean warmup;
	String status = "not_started";
	
	
	boolean killall = false; // use this if the server is dead or it has crashed
	
    
    public void connect(String HOST, int PORT, boolean SSL)
    {
    	this.HOST = HOST;
    	this.PORT = PORT;
    	this.ssl = SSL;
    }
    
    public ChannelFuture disconnect()
    {
    	    ChannelFuture channelFuture = f.channel().close().awaitUninterruptibly();
    	    //you have to close eventLoopGroup as well
    	    group.shutdownGracefully();
    	    return channelFuture;
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
				logger.info(">> STARTING client in MODE Blocking? " + blocking);
				if(blocking)
					group = new OioEventLoopGroup();
				else
					group = new NioEventLoopGroup();
		        ChannelPipeline p = null;
		        Client nc = new Client();
		        TCPChannelInitializer nci = new TCPChannelInitializer();
		        nci.setClient(this);
		        try {
		            Bootstrap b = new Bootstrap();
		            if(blocking)
		            {
		            	b.group(group)
			             .channel(OioSocketChannel.class)
			             .option(ChannelOption.TCP_NODELAY, true)
			             .option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(800*1024, 1024*1024))
			             //.option(ChannelOption.SO_BACKLOG, 100)
	
			             //.option(ChannelOption.TCP_NODELAY, true)
			             //.option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, (1024*1024))		             
			             //.option(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, (512*1024))
			             
			             .handler(nci);
		            }
		            else
		            {
			            b.group(group)
			             .channel(NioSocketChannel.class)
			             .option(ChannelOption.TCP_NODELAY, true)
			             .option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(800*1024, 1024*1024))
			             //.option(ChannelOption.SO_BACKLOG, 100)

			             //.option(ChannelOption.TCP_NODELAY, true)
			             //.option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, (1024*1024))		             
			             //.option(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, (512*1024))
			             
			             .handler(nci);
		            }
		            nc.f = b.connect(HOST, PORT).sync();          
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
		            // Wait until the connection is closed.
		            nc.f.channel().closeFuture().sync();
		        } finally {
		            // Shut down the event loop to terminate all threads.
		            group.shutdownGracefully();
		        }
	    	}catch(Exception ex)
	    	{
	    		attempt++;
	    		
	    		logger.info("Attempting Number " + attempt);
	    		// see if sleeping helps ?
	    		try
	    		{
	    			// sleeping only for 1 second here
	    			// but the py executor sleeps in 2 second increments
	    			Thread.sleep(attempt*SLEEP_TIME);
	    		}catch(Exception ex2)
	    		{
	    			
	    		}
	    	}
    	}
    	
    	if(attempt > 6)
            logger.info("CLIENT Connection Failed !!!!!!!");
    	
        //warmup();


    }	
    
    public boolean isReady()
    {
    	return this.ready;
    }
 
    

    public Object executeCommand(PayloadStruct ps)
    {
    	if(killall) {
        	throw new SemossPixelException("Analytic engine is no longer available. This happened because you exceeded the memory limits provided or performed an illegal operation. Please relook at your recipe");
    	}
    	
    	int attempt = 0;
    	String id = "ps"+ count.getAndIncrement();
    	ps.epoc = id;
    	
    	
    	while(ctx == null && attempt < 6)
    	{
    		logger.info("R or Python not yet available.. will sleep" + attempt);
    		try {
    			Thread.sleep(attempt * 500);
    			attempt++;
    		} catch(Exception ignored) {
    			// ignored
    		}
    	}
    	// need a way to kill this thread as well
    	if(ctx == null) {
    		logger.info( "Connection failed to get the context.!! ");
    	}
    	//else
    	//	logger.info("Context is set !!");
    	
    	synchronized(ps) // going back to single threaded .. earlier it was ps
    	{	
    		//if(ps.hasReturn)
    		requestMap.put(id, ps);

    		writePayload(ps);
	    	// send the message
			
    		// time to wait = average time * 10
    		
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

			// after 10 seconds give up
			//printUnprocessed();
			return responseMap.remove(ps.epoc);
    	}
    }
    
    private void writePayload(PayloadStruct ps)
    {
    	while(!ctx.channel().isWritable())
    	{
    		try
    		{
    			Thread.sleep(200);
    		}catch(Exception ignored)
    		{
    			
    		}
    	}
    	//if(ctx.channel().isWritable())
    	{
	    	byte [] bytes = FstUtil.serialize(ps);
			logger.info("Firing operation " + ps.methodName + " with payload length >> " + bytes.length +"   " + ps.epoc + " Writeable ?" + ctx.channel().isWritable() + " Current write index " + ctx.channel().bytesBeforeWritable());
			logger.info("Low: " + ctx.channel().config().getWriteBufferLowWaterMark() +  " <> High: " + ctx.channel().config().getWriteBufferHighWaterMark());
			ByteBuf buff = Unpooled.buffer(bytes.length);
			buff.writeBytes(bytes);
			ChannelFuture cf = ctx.write(buff);
			
			ctx.flush();
			if(buff.refCnt() > 1)
				buff.release();
			buff = null;
    	}
    }
    
    private void writeEmptyPayload()
    {
    	PayloadStruct ps = new PayloadStruct();
    	ps.epoc="0000";
    	ps.methodName = "EMPTYEMPTYEMPTY";
    	writePayload(ps);
    }
    
    
    
    public void stopPyServe(String dir)
    {
    	PayloadStruct ps = new PayloadStruct();
    	ps.methodName = "CLOSE_ALL_LOGOUT<o>";
    	writePayload(ps);
    	
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
    
    public void crash(boolean close)
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
    	
    	if(close) {
    		throw new SemossPixelException("Analytic engine has been programmatically closed (likely due to session being closed)");
    	} else {
    		throw new SemossPixelException("Analytic engine is no longer available. This happened because you exceeded the memory limits provided or performed an illegal operation. Please relook at your recipe");
    	}
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
    
}
