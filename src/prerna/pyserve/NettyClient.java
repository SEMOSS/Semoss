package prerna.pyserve;

import java.util.HashMap;
import java.util.Map;

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
	public ChannelHandlerContext ctx = null;
    static final int SIZE = Integer.parseInt(System.getProperty("size", "256"));
    Object lock = new Object();
    Object response = null;
    String HOST = null;
    int PORT = -1;
    boolean ssl = false;
    Map responseMap = new HashMap();
    
    public void connect(String HOST, int PORT, boolean SSL)
    {
    	this.HOST = HOST;
    	this.PORT = PORT;
    	this.ssl = SSL;
    }

    public void run()	
    {
        // Configure SSL.git
    	boolean connected = false;
    	int attempt = 1;
    	while(!connected && attempt < 6)
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
		        EventLoopGroup group = new NioEventLoopGroup();
		        ChannelPipeline p = null;
		        NettyClient nc = new NettyClient();
		        TCPChannelInitializer nci = new TCPChannelInitializer();
		        nci.setClient(this);
		        try {
		            Bootstrap b = new Bootstrap();
		            b.group(group)
		             .channel(NioSocketChannel.class)
		             .option(ChannelOption.TCP_NODELAY, true)
		             .handler(nci);
		
		            nc.f = b.connect(HOST, PORT).sync();          
		            System.out.println("CLIENT Connection complete !!!!!!!");
		            connected = true;
		            // Wait until the connection is closed.
		            nc.f.channel().closeFuture().sync();
		        } finally {
		            // Shut down the event loop to terminate all threads.
		            group.shutdownGracefully();
		        }
	    	}catch(Exception ex)
	    	{
	    		attempt++;
	    		System.err.println("Attempting Number " + attempt);
	    		// see if sleeping helps ?
	    		try
	    		{
	    			Thread.sleep(attempt * 1000);
	    		}catch(Exception ex2)
	    		{
	    			
	    		}
	    	}
    	}
    	
    	if(attempt > 6)
            System.out.println("CLIENT Connection Failed !!!!!!!");

    }	
    
    public Object executeCommand(String command)
    {
    	int attempt = 1;
    	while(ctx == null && attempt < 6)
    	{
    		try
    		{
    			Thread.sleep(attempt * 1000);
    			attempt++;
    		}catch(Exception ignored)
    		{
    			
    		}
    	}
    	if(ctx == null)
    		return "Connection failed to get the context.!! ";
    		
    	TCPCommandeer tc = new TCPCommandeer();
    	tc.ctx = ctx;
    	tc.command = command;
    	tc.nc = this;
    	Thread t = new Thread(tc);

    	//while(!responseMap.containsKey(command))
    	{
	    	synchronized(lock)
	    	{
	    		try
	    		{
	    	    	t.run();
	    			lock.wait();
	    			//System.out.println("Object that came " + response);
	    			Object [] outputObj = (Object [])response;		
	    			responseMap.put(outputObj[0], outputObj[1]);
	    		}catch(Exception ex)
	    		{
	    			
	    		}
	    	}
    	}

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
