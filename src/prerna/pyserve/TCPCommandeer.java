package prerna.pyserve;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;

public class TCPCommandeer implements Runnable {
	
	
	// takes command and executes it
	// quite simple
	String command = null;
	ChannelHandlerContext ctx = null;
	NettyClient nc = null;
	
	public String file = null;
	int sleepTime = 50; // milliseconds

	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
			//synchronized(nc.lock)
			{
				//Thread.sleep(sleepTime);
		    	command = command + "<o>";
		    	byte [] bytes = command.getBytes();
				ByteBuf buff = Unpooled.buffer(bytes.length);
				buff.writeBytes(bytes);
				ctx.writeAndFlush(buff);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
