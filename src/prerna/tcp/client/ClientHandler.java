package prerna.tcp.client;

import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import prerna.tcp.PayloadStruct;
import prerna.util.FstUtil;

public class ClientHandler extends ChannelInboundHandlerAdapter //MessageToMessageDecoder
{

    private final ByteBuf firstMessage;
    String inputSoFar = "";
    String endChar = "<o>";
    
    int readerIndex = 0;
    int copyIndex = 0;
    int offset = 4;
    byte [] bytes = null;
    ByteBuf buf = null;
    int totalBytes = 0;
    int filledIndex = 0;
    int loop = 1;
    
    Client nc = null;

    /**
     * Creates a client-side handler.
     */
    public ClientHandler() {
        firstMessage = Unpooled.buffer(Client.SIZE);
        for (int i = 0; i < firstMessage.capacity(); i ++) {
            firstMessage.writeByte((byte) i);
        }
    }

    public void setClient(Client nc)
    {
    	this.nc = nc;
    }
    
    @Override
    public void channelActive(ChannelHandlerContext ctx) {
    	nc.ctx = ctx;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Close the connection when an exception is raised.
        cause.printStackTrace();
        ctx.close();
        
        // need to also inform all the users back
        nc.crash(false);
    }
	
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg)
	{
		ByteBuf in = (ByteBuf)msg;
		
		if(buf == null)
		{
			//System.err.print("." + in.readableBytes());
			buf = in.copy();
			byte [] lengthBytes = new byte[4];
			buf.readBytes(lengthBytes);
			String lengthStr = new String(lengthBytes);

			bytes = new byte[4];
    	   	ByteBuffer wrapped = ByteBuffer.wrap(lengthBytes); // big-endian by default
    	   	totalBytes = wrapped.getInt(); // 1
    	   	System.out.println(">> Total Bytes " + totalBytes);
		}
		else		
		{
			int curLen = buf.capacity();
			int inLen = in.readableBytes();
			int newLen = curLen + inLen;
			System.err.print("." + inLen + "+" + curLen);
			buf = buf.capacity(newLen);
			//System.err.println("New Buf capacity " + buf.capacity());
			in.getBytes(0, buf, curLen, in.readableBytes());
			buf.writerIndex(newLen);
			//System.err.println("bytes so far " + buf.capacity());

		}
		in.release();		
		//System.err.println("Channel Read " + "Readable byte " + buf.readableBytes() + "  index " + buf.readerIndex() + "  writer " + buf.writerIndex());

		// try to see if we have enough to trigger
		//System.err.println("Comparing.. " + buf.capacity() + "<<>>" + totalBytes);
		if(buf.readableBytes() >= totalBytes)
		{
			byte[]  data = new byte[totalBytes];			
			buf.readBytes(data);
			PayloadStruct ps = (PayloadStruct)FstUtil.deserialize(data);
			System.out.println("Ps epoc " + ps.epoc + " Method " + ps.methodName);
	    	try
	    	{
	    		printFinalOutput(ps);
	    	}catch (Exception ex)
	    	{
	    		ex.printStackTrace();
	    	}
		}
	}
	
	public void printFinalOutput(PayloadStruct ps)
	{
		//System.out.println(" Print .. Reading bytes .." + buf.readableBytes() + " <<>>" + totalBytes);
		try
		{
			//if(buf.readableBytes() >= totalBytes)
			{
				if(ps.ex != null)
				{
					System.out.println("Payload came with an exception " + ps.ex);
					//throw ps.ex;
				}
				
				if(ps.payload != null)
				{
					//System.out.println("Got the response for  " + ps.methodName + "  " + ps.epoc);
				}
				String id = ps.epoc;
				
				// why is this even coming here ?
				
				PayloadStruct lock = (PayloadStruct)nc.requestMap.remove(id);
				
				// put it in response
				nc.responseMap.put(id, ps);
				
				
				if(lock != null)
				{
					synchronized(lock)
					{
						lock.notifyAll();
					}
				}
			}		
		}catch(Exception ex)
		{
			ex.printStackTrace();
		}
		
		// see if there is more
		if(buf.readableBytes() > 0)
		{
			ByteBuf buf2 = buf.copy(buf.readerIndex(), buf.readableBytes());
			//System.err.println("Capacity .. " + buf.capacity());
			if(buf.refCnt() > 0)
				buf.release();
			buf = buf2;
			bytes = new byte[4];
			buf.readBytes(bytes);
    	   	ByteBuffer wrapped = ByteBuffer.wrap(bytes); // big-endian by default
    	   	totalBytes = wrapped.getInt(); // 1
    	   //	System.out.println(" <<  Total Bytes " + totalBytes);
    		//System.err.println("Post processing - After Copy " + "Readable byte " + buf.readableBytes() + "  index " + buf.readerIndex() + "  writer " + buf.writerIndex());
    		
    		// so there is a possibility here where the readableBytes is already available
    		if(buf.readableBytes() >= totalBytes) // hopefully this does it
    		{
    			//System.err.println("This is where it might cluster");
    			byte[]  data = new byte[totalBytes];			
    			buf.readBytes(data);
    			PayloadStruct ps2 = (PayloadStruct)FstUtil.deserialize(data);
    			printFinalOutput(ps2);
    		}
		}
		else
		{
			totalBytes = 0;
			if(buf.refCnt() > 0)
				buf.release();
			buf = null;
		}
	}

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
    	
    	//ctx.fireChannelRead(ctx);
       ctx.flush();
    }

}
