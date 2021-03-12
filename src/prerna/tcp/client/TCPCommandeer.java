package prerna.tcp.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import prerna.tcp.PayloadStruct;
import prerna.util.FstUtil;

public class TCPCommandeer implements Runnable {
	
	
	// takes command and executes it
	// quite simple
	String command = null;
	ChannelHandlerContext ctx = null;
	Client nc = null;
	String endChar = null;
	PayloadStruct ps = null;
	
	public String file = null;
	int sleepTime = 50; // milliseconds

	public TCPCommandeer()
	{
		
	}
	
	public TCPCommandeer(String endChar)
	{
		this.endChar = endChar;
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
			//synchronized(nc.lock)
			if(ps == null)
			{
				//Thread.sleep(sleepTime);
		    	command = command + endChar;
		    	byte [] bytes = command.getBytes();
				ByteBuf buff = Unpooled.buffer(bytes.length);
				buff.writeBytes(bytes);
				ctx.writeAndFlush(buff);
				buff.release();
			}
			else
			{
				byte [] bytes = FstUtil.serialize(ps);
				ByteBuf buff = Unpooled.buffer(bytes.length);
				buff.writeBytes(bytes);
				ctx.writeAndFlush(buff);				
				buff.release();
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
