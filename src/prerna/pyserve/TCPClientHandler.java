package prerna.pyserve;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.nustaq.serialization.FSTObjectInput;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class TCPClientHandler extends ChannelInboundHandlerAdapter {

    String inputSoFar = "";
    String endChar = "<o>";
    
    NettyClient nc = null;
    ChannelHandlerContext ctx = null;

    byte [] bytes = null;
    int offset = 4;
    int readerIndex = 0;
    int totalBytes = 0;
    int copyIndex = 0;
    ByteBuf buf = null;
    int bufLen = 0;
    
    
	public static Logger LOGGER = LogManager.getLogger(TCPClientHandler.class.getName());

    /**
     * Creates a client-side handler.
     */
    public TCPClientHandler() {

    }

    public void setClient(NettyClient nc)
    {
    	this.nc = nc;
    }
    
    @Override
    public void channelActive(ChannelHandlerContext ctx) 
    {
    	nc.ctx = ctx;
    }
    
    @Override
    public void channelRead(ChannelHandlerContext cts, Object msg)
    {
		ByteBuf in = (ByteBuf)msg;
		if(buf == null)
		{
			//System.err.print("." + in.readableBytes());
			buf = in.copy();
		}
		else		
		{
			int curLen = buf.capacity();
			int inLen = in.readableBytes();
			int newLen = curLen + inLen;
			//System.err.print("." + inLen + "+" + curLen);
			buf = buf.capacity(newLen);
			in.getBytes(0, buf, curLen, in.readableBytes());
		}
		in.release();
    }

    
	public void setFinalOutput()
	{
		offset = 0; // set this to 4 if there is a length appender
		if(buf != null)
		{
			byte[]  data = new byte[buf.capacity() - offset];
			// removing the length appender
			//byte[]  data = new byte[buf.readableBytes()];
			buf.getBytes(offset, data);
			// remove the first four bytes.. but we will get to it
			//System.err.println("Total size .. " + data.length);
	    	ByteArrayInputStream bais = new ByteArrayInputStream(data);
	    	
			
	    	try {
	    		
				FSTObjectInput fi = new FSTObjectInput(bais);
				Object retObject = fi.readObject();
	
				/*
				ObjectInputStream ois = new ObjectInputStream(bais);
				Object retObject = ois.readObject();
				*/
				
				if(retObject != null)
				{
					nc.response = retObject;
				}
				else
					System.out.println("Return was a null");
				
	
	    	}catch(Exception ex)
	    	{
	    		ex.printStackTrace();
	    	}finally
	    	{
	    		buf.release();
	    		buf = null;
	    	}
		}
	}

    

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
    	
    	//System.err.println("====================== read complete ====================== " );
    	setFinalOutput();
    	synchronized(nc.lock)
    	{
    		
    		nc.lock.notifyAll();
    	}        				
    	ctx.fireChannelRead(ctx);
       ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Close the connection when an exception is raised.
        cause.printStackTrace();
        ctx.close();
    }
}
