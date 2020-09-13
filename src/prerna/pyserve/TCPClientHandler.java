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
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
    	
    	
    	//System.out.println("///////////////////////////////////////////");
    	//System.err.println("Channel read called");
        //ctx.write(msg);
    	//System.out.println("Came into channel read " + msg);
    	// I have to go byte by byte
    	// assimilate it and then do what i need to do
    		ByteBuf in = (ByteBuf) msg;
    		buf = in;
    		// step 1 get the 4 bytes and see how much data is there
        	if(bytes == null) // first time
        		bytes = new byte[4];

            if(offset == 4) // first time
            {
            	in.getBytes(readerIndex, bytes);
        	   	ByteBuffer wrapped = ByteBuffer.wrap(bytes); // big-endian by default
        	   	totalBytes = wrapped.getInt(); // 1
        	
        	   	//LOGGER.info("Size right now is" + totalBytes + in.readableBytes());
        	   	
        	   	//System.out.println("Size right now is" + totalBytes + "=====<<>>====" + in.readableBytes());
        	   	
        	   	bytes = new byte[totalBytes]; // this is just for testing
        	   	readerIndex = offset;
        	   	//offset = 0;
            }
            
            if(readerIndex == 0)
            	readerIndex = in.readerIndex();
        	
            //LOGGER.info("Reader Index set to.. " + readerIndex);
    		int bytesAvailable = in.readableBytes();
    		
            //LOGGER.info("Bytes Available" + bytesAvailable + " ;;; required is " + totalBytes);

            //if(totalBytes + 4 == bytesAvailable)
    		{
            	
        		// first time
        		// available = say 80
        		// readerIndex = offset
        		// byte array = available - offet
        		// read into a byte - 0
        		// copy into main array
        		
        		// move the readerindex to offset + byteArray length
        		// move copy index - bytearray.length
        		// offset = 0
            	// 35 bytes available
            	// I already have 34 copyIndex
            	// I only need 35 - 34 = 1
     
            	// totalBytes 20 bytes -----------------------
            	// first time bytes that came 14 + 4 for length
            	// copyIndex = 0
            	// fill from copyIndex ... length 18
            	// copy index = 14
            	// next time - I get remaining bytes
            	// need to fill - total bytes - copyIndex
            	
            	
            	int bytesRemaining = totalBytes - copyIndex;

            	// fill current array
                LOGGER.info("Bytes Available" + bytesAvailable);
                
                readerIndex = offset;
                
            	int actualAvailable = bytesAvailable - offset;
            	byte [] curAvl = new byte[actualAvailable];
            	
            	//System.err.println("Reader index " + readerIndex + "  <> actual available " + actualAvailable + " <> offset :" + offset);
            	
        		in.getBytes(readerIndex, curAvl); //, byteIndex, avl);
            	
            	// see if the actuallFill > bytesRemaining if so do bytes remaining
            	// else replace actual fill
            	
           		//System.err.println("Byte Index is at..  " + copyIndex + " <<>> " + bytes.length + " <<>> " + actualAvailable);

        		System.arraycopy(curAvl, 0, bytes, copyIndex, actualAvailable);
        		
        		copyIndex = copyIndex + actualAvailable;
        		offset = 0;
        		readerIndex = 0;
        		
            	if(copyIndex == bytes.length)
            	{
        	    	ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        	    	/*
        			FSTObjectInput fi = new FSTObjectInput(bais);
        			Object retObject = fi.readObject();
        			*/
        	    	try {
        	    		
            			FSTObjectInput fi = new FSTObjectInput(bais);
            			Object retObject = fi.readObject();

        				/*ObjectInputStream ois = new ObjectInputStream(bais);
        				Object retObject = ois.readObject();
        				*/
            			
        				if(retObject != null)
        					System.out.println("FST Output is [" + retObject + "]");
        				else
        					System.out.println("Return was a null");

        		    	nc.response = retObject;

        			    readerIndex = 0;
        			    copyIndex = 0;
        			    offset = 4;
        			    bytes = null;
        			    totalBytes = 0;
        			    /*
        		    	synchronized(nc.lock)
        		    	{
        		    		nc.lock.notifyAll();
        		    	} 
        		    	*/       				
        			} catch (ClassNotFoundException e) {
        				// TODO Auto-generated catch block
        				e.printStackTrace();
        			} catch (IOException e) {
        				// TODO Auto-generated catch block
        				e.printStackTrace();
        			}    	    	
    		}
    	}

      	// old method
    		/*	        
	    	int readerIndex = in.readerIndex();
	    	in.getBytes(readerIndex, bytes);
	    
			try {
				
				
				// receive through regular object
		    	ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
		    	ObjectInputStream ois;
				ois = new ObjectInputStream(bais);
		    	System.out.println("Byte Array Output is " + ois.readObject());
		    	
				
				
		    	// receive through FST
		    	ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
		    	
				FSTObjectInput fi = new FSTObjectInput(bais);
				Object retObject = fi.readObject();
				
		    	ObjectInputStream ois = new ObjectInputStream(bais);
		    	Object retObject = ois.readObject();
				
				if(retObject != null)
					System.out.println("FST Output is [" + retObject + "]");
				else
					System.out.println("Return was a null");
				
		    	nc.response = retObject;
		    	
		    	
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			nc.lock.notifyAll();
    	}
    	
    	
        while (in.isReadable()) { // (1)
            inputSoFar = inputSoFar + (char) in.readByte();
            if(inputSoFar.endsWith(endChar))
            {            	
            	inputSoFar = inputSoFar.replace(endChar, "");
            	System.out.print("RESPONSE >> \n" + inputSoFar + "\n<<");
                //ctx.writeAndFlush(inputSoFar);
                //ctx.flush();            
                inputSoFar = "";
            }
        }
        */
   }
    

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
    	
    	System.err.println("====================== read complete ====================== " );
    	synchronized(nc.lock)
    	{
    		
    		nc.lock.notifyAll();
    	}        				
    	/*
    	if(buf != null)
    	{
    		System.err.println(">>>>" + buf.readableBytes());
    		
    		bytes = new byte[buf.readableBytes()];
    		buf.getBytes(4, bytes);
    		
	    	ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
	    	/*
			FSTObjectInput fi = new FSTObjectInput(bais);
			Object retObject = fi.readObject();
			
	    	try {
				ObjectInputStream ois = new ObjectInputStream(bais);
				Object retObject = ois.readObject();
				
				if(retObject != null)
					System.out.println("FST Output is [" + retObject + "]");
				else
					System.out.println("Return was a null");

		    	//nc.response = retObject;
				Object [] objArr = (Object[])retObject;
				System.out.println("Output..  " + objArr[1]);

			    readerIndex = 0;
			    copyIndex = 0;
			    offset = 4;
			    bytes = null;
			    totalBytes = 0;

				//nc.lock.notifyAll();
				
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}    	    	
    		buf = null;
    	}*/
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
