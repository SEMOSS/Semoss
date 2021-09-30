package prerna.tcp.client;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Vector;

import org.apache.logging.log4j.Logger;

import prerna.tcp.PayloadStruct;
import prerna.util.FstUtil;

public class SocketClientHandler implements Runnable
{

    String inputSoFar = "";
    String endChar = "<o>";
    
	int offset = 4;
	int totalBytes = 0;
	List<ByteBuffer> inputs = new Vector<ByteBuffer>();
	
	boolean done = false;    
	InputStream in = null;

	byte[] lenBytes = null;
	int lenBytesReadSoFar = 0;
	byte[] curBytes = null;
	int bytesReadSoFar = 0;
	
    
    Client nc = null;
	public  Logger LOGGER = null;

    // I think we should move this also into stream reader or move stream reader here

    public void setClient(Client nc)
    {
    	this.nc = nc;
    }
    

	
	public void printObject(Object obj)
	{
		// we know this is a payload struct
		// just print it
		PayloadStruct ps = (PayloadStruct)obj;
		//System.err.println("<< Payload " + ps.epoc + " bytes left " + totalBytes);
		
		// this is where we inform the nc that this is done
		// will come to it 
		try
		{
			if(ps != null)
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
	}
	
	public void setInputStream(InputStream in)
	{
		this.in = in;
	}
	
	public void setLogger(Logger LOGGER)
	{
		this.LOGGER = LOGGER;
	}
	
	@Override
	public void run()
	{
		while(!done)
		{
			try
			{
				int bytesToRead = offset;
				int readBytes = 0;
				if(lenBytes != null)
				{
					bytesToRead  = ByteBuffer.wrap(lenBytes).getInt();
					if(curBytes == null)
						curBytes = new byte[bytesToRead]; // initialize only if it is not null

					readBytes = in.read(curBytes, bytesReadSoFar, (curBytes.length - bytesReadSoFar)); // block
					LOGGER.info("  Need bytes " + curBytes.length  + " <> Got bytes " + readBytes);
					bytesReadSoFar = bytesReadSoFar + readBytes;
					
					if(bytesReadSoFar == curBytes.length && readBytes != -1)
					{
						try
						{
							Object retObject = FstUtil.deserialize(curBytes);
							if(retObject != null)
							{
								printObject(retObject);
								lenBytes = null;
								bytesReadSoFar = 0;
								lenBytesReadSoFar = 0;
								curBytes = null;
							}
							else
							{
								LOGGER.info("Failed to deserialize " + curBytes.length + " <> bytes read " + readBytes);								
							}
						}catch(Exception ex)
						{
							LOGGER.info("Failed to deserialize " + curBytes.length + " <> bytes read " + readBytes);
						}
					}
				}
				else
				{
					if(lenBytes == null)
						lenBytes = new byte[bytesToRead]; // block it
					int bytesRead = in.read(lenBytes, lenBytesReadSoFar, (lenBytes.length - lenBytesReadSoFar)); // block
					lenBytesReadSoFar = lenBytesReadSoFar + bytesRead;
				}	
				
				if(readBytes < 0) // stream is closed kill this thread
				{
					done = true;
					this.nc.connected = false;
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				System.err.println("Server Stream is closed!!");
				done = true; // ba bye
				// at some point we can relisten if we want.. 
			}
		}
	}



}
