package prerna.pyserve;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

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
	
	
}
