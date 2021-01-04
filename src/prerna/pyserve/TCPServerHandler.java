package prerna.pyserve;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.nustaq.serialization.FSTObjectOutput;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoopGroup;
import prerna.ds.py.PyExecutorThread;

public class TCPServerHandler extends ChannelInboundHandlerAdapter {
	
	String inputSoFar = new String();
	
	String endChar = "<o>";
	ByteBuf buf = null;
	
	PyExecutorThread pt = null;
	String mainFolder = null;
	boolean test = false;
	
	EventLoopGroup bossGroup = null;
	EventLoopGroup workerGroup = null;
	
	
	public static Logger LOGGER = null;
		
	public void setPyExecutorThread(PyExecutorThread pyt)
	{
		this.pt = pyt;
	}
	
	public void setMainFolder(String mainFolder)
	{
		this.mainFolder = mainFolder;
	}
	
	public void setLogger(Logger LOGGER)
	{
		this.LOGGER = LOGGER;
	}


    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
    	
    	System.err.println("THread runing is " + Thread.currentThread().getId());
    	LOGGER.info(".");
    	
    	try {
			ByteBuf in = (ByteBuf) msg;
			// read it
			byte[] bytes = new byte[in.readableBytes()];
			
			int readerIndex = in.readerIndex();
			in.getBytes(readerIndex, bytes);
			
			String command = new String(bytes);
			
			inputSoFar = inputSoFar + command;
			
			byte [] output = null;
			{
				if(inputSoFar.endsWith(endChar))
				{
					inputSoFar = inputSoFar.replace(endChar, "");
					if(inputSoFar.contains("CLOSE_ALL_LOGOUT"))
					{
						LOGGER.info("Closing all comms.. processing logout");
						
						cleanUp(ctx);
					}
					else
					{
						// execute the command
						Object retObject = processCommand(inputSoFar);
						
						if(retObject == null)
						{
							LOGGER.debug("output.. [" + retObject + "]" + "for command" + command);
							retObject = "";
						}
						else
							LOGGER.info("Got the result");
						output = marshalOutput(inputSoFar, retObject);
					}
				}
			}
				
			if(output != null)
			{
				ByteBuf buff = Unpooled.buffer(output.length);
				buff.writeBytes(output);
	
				ctx.writeAndFlush(buff);
				ctx.flush();
				inputSoFar = "";
				LOGGER.info("Result Flushed");
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    }
    
    
    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
    	//processData(ctx);
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Close the connection when an exception is raised.
        //cause.printStackTrace();
    	// connection has been closed by the host
    	// may be tomcat
    	
		cleanUp(ctx);
        ctx.close();
    }
    
    public void cleanUp(ChannelHandlerContext ctx)
    {
    	if(!test)
    	{
			pt.keepAlive = false;
			processCommand("'logout now'"); // this should trigger it and kill it

			// stop the logger
			LogManager.shutdown();

			ctx.channel().close();
			ctx.channel().parent().close();

			bossGroup.shutdownGracefully();
			workerGroup.shutdownGracefully();
			
			try {
				FileUtils.deleteDirectory(new File(mainFolder));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}	
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
    
    
    public byte [] marshalOutput(String input, Object output)
    {
		try {
			Object [] outputObj = new Object[2];
			outputObj[0] = inputSoFar;
			outputObj[1] = output;
			
			// write it back
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			
			// FST
			
			FSTObjectOutput fo = new FSTObjectOutput(baos);
			fo.writeObject(outputObj);
			fo.close();
			
			return baos.toByteArray();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
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

    // try methods - not used
    
    //@Override
    public void channelRead2(ChannelHandlerContext cts, Object msg)
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

}