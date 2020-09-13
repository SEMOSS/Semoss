package prerna.pyserve;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Logger;
import org.nustaq.serialization.FSTObjectOutput;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import prerna.ds.py.PyExecutorThread;

public class TCPServerHandler extends ChannelInboundHandlerAdapter {
	
	String inputSoFar = new String();
	
	String endChar = "<o>";
	
	PyExecutorThread pt = null;
	String mainFolder = null;
	
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
    	
    	try {
			ByteBuf in = (ByteBuf) msg;
			// read it
			byte[] bytes = new byte[in.readableBytes()];
			
			int readerIndex = in.readerIndex();
			in.getBytes(readerIndex, bytes);
			
			String command = new String(bytes);
			
			inputSoFar = inputSoFar + command;
			if(inputSoFar.endsWith(endChar))
			{
				inputSoFar = inputSoFar.replace(endChar, "");
				if(inputSoFar.contains("CLOSE_ALL_LOGOUT"))
				{
					LOGGER.info("Closing all comms.. processing logout");
		
					pt.keepAlive = false;
					processCommand("'logout now'"); // this should trigger it and kill it
					
					ctx.channel().close();
					ctx.channel().parent().close();
					
					// stop the logger
					//LogManager.resetConfiguration();
					
					FileUtils.deleteDirectory(new File(mainFolder));
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
					Object [] outputObj = new Object[2];
					outputObj[0] = inputSoFar;
					outputObj[1] = retObject;
					
					// write it back
					ByteArrayOutputStream baos = new ByteArrayOutputStream();
					
					// FST
					
					FSTObjectOutput fo = new FSTObjectOutput(baos);
					fo.writeObject(outputObj);
					fo.close();
					
					// Java Object
					/*
					ObjectOutputStream oos = new ObjectOutputStream(baos);
					oos.writeObject(outputObj);
					//oos.writeChars("<o>");
					oos.flush();
					oos.close();
					*/
					
					byte [] output = baos.toByteArray();
					
					ByteBuf buff = Unpooled.buffer(output.length);
					buff.writeBytes(output);
		
					ctx.writeAndFlush(buff);
					ctx.flush();
					inputSoFar = "";
					LOGGER.info("Result Flushed");

				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    }
    
    
    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Close the connection when an exception is raised.
        cause.printStackTrace();
        ctx.close();
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
				monitor.wait(4000);
				response = this.pt.response.get(command);
			} catch (Exception ex) {
				LOGGER.debug(ex);
				response = ex.getMessage();
			}
		}
		return response;
    }
}