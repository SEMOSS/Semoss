package prerna.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.nustaq.serialization.FSTObjectInput;
import org.nustaq.serialization.FSTObjectOutput;

public class FstUtil 
{

	public static byte[] serialize(Object input)
	{
		try {
			
			// write it back
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			
			// FST
			
			FSTObjectOutput fo = new FSTObjectOutput(baos);
			fo.writeObject(input);
			fo.close();
			
			return baos.toByteArray();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;		
	}
	
	public static Object deserialize(byte[] data)
	{
    	ByteArrayInputStream bais = new ByteArrayInputStream(data);
    	try {
    		
			FSTObjectInput fi = new FSTObjectInput(bais);
			Object retObject = fi.readObject();
			return retObject;
    	}catch(Exception ex)
    	{
    		ex.printStackTrace();
    	}
    	return null;
	}
	
	public static byte[] packBytes(Object obj) {
		byte[] psBytes = FstUtil.serialize(obj);

		// get the length
		int length = psBytes.length;

		// make this into array
		byte[] lenBytes = ByteBuffer.allocate(4).putInt(length).array();

		// pack both of these
		byte[] finalByte = new byte[psBytes.length + lenBytes.length];

		for (int lenIndex = 0; lenIndex < lenBytes.length; lenIndex++)
			finalByte[lenIndex] = lenBytes[lenIndex];

		for (int lenIndex = 0; lenIndex < psBytes.length; lenIndex++)
			finalByte[lenIndex + lenBytes.length] = psBytes[lenIndex];

		return finalByte;
	}

	
	
	
	
}
