package prerna.pyserve;

import java.util.HashMap;
import java.util.Map;

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
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

public class NettyClient implements Runnable{

	ChannelFuture f = null;
	EventLoopGroup group = null;
	public ChannelHandlerContext ctx = null;
    static final int SIZE = Integer.parseInt(System.getProperty("size", "256"));
    Object lock = new Object();
    Object response = null;
    String HOST = null;
    int PORT = -1;
    boolean ssl = false;
    Map responseMap = new HashMap();
    boolean ready = false;
	private static final String CLASS_NAME = NettyClient.class.getName();
	private static final Logger logger = LogManager.getLogger(CLASS_NAME);

    
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
    	boolean connected = false;
    	int attempt = 1;
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
		        group = new NioEventLoopGroup();
		        ChannelPipeline p = null;
		        NettyClient nc = new NettyClient();
		        TCPChannelInitializer nci = new TCPChannelInitializer();
		        nci.setClient(this);
		        try {
		            Bootstrap b = new Bootstrap();
		            b.group(group)
		             .channel(NioSocketChannel.class)
		             .option(ChannelOption.TCP_NODELAY, true)
		             .option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, (1024*1024))		             
		             .option(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, (512*1024))
		             .handler(nci);
		
		            nc.f = b.connect(HOST, PORT).sync();          
		            logger.info("CLIENT Connection complete !!!!!!!");
		            Thread.sleep(100); // sleep some before executing command
		            // prime it
		            logger.info("First command.. Prime" + executeCommand("2+2"));
		            connected = true;
		            ready = true;
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
	    			Thread.sleep(attempt*300);
	    		}catch(Exception ex2)
	    		{
	    			
	    		}
	    	}
    	}
    	
    	if(attempt > 6)
            logger.info("CLIENT Connection Failed !!!!!!!");

    }	
    
    public boolean isReady()
    {
    	return this.ready;
    }
    
    public Object executeCommand(String command)
    {
    	int attempt = 1;
    	// not sure if this is required anymore
    	// this is already done in the user
    	
    	while(ctx == null && attempt < 6)
    	{
    		logger.info("Python not yet available.. will sleep" + attempt);
    		try
    		{
    			Thread.sleep(attempt * 500);
    			attempt++;
    		}catch(Exception ignored)
    		{
    			
    		}
    	}
    	if(ctx == null)// need a way to kill this thread as well
    		return "Connection failed to get the context.!! ";
    		
    	TCPCommandeer tc = new TCPCommandeer();
    	tc.ctx = ctx;
    	tc.command = command;
    	tc.nc = this;
    	Thread t = new Thread(tc);
    	response = null;

    	
    	synchronized(lock)
    	{
    		try
    		{
    	    	t.run();
    	    	boolean done = false;
    	    	while(!done) //attempt < 6)
    	    	{
	    			lock.wait(); // this waits indefeinitely
	    			//System.out.println("Object that came " + response);
	    			if(response != null)
	    			{
	    				Object [] outputObj = (Object [])response;		
	    				responseMap.put(outputObj[0], outputObj[1]);
	    				done = true;
	    				break;
	    			}
	    			attempt++;
    	    	}
    		}catch(Exception ex)
    		{
    			
    		}
    	}
    	//if(attempt > 5)
    	//	return "Output has taken way longer than expected, removing block";
    	
    	return responseMap.remove(command);
    }
    
    
    public void stopPyServe(String dir)
    {
    	String command = "CLOSE_ALL_LOGOUT<o>";
    	byte [] bytes = command.getBytes();
		ByteBuf buff = Unpooled.buffer(bytes.length);
		buff.writeBytes(bytes);
		ctx.writeAndFlush(buff);

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
    
    
}
