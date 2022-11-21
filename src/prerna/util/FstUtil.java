package prerna.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.nustaq.serialization.FSTObjectInput;
import org.nustaq.serialization.FSTObjectOutput;

public class FstUtil {

	private static final Logger logger = LogManager.getLogger(FstUtil.class);

	public static byte[] serialize(Object input) {
		ByteArrayOutputStream baos = null;
		FSTObjectOutput fo = null;
		try {
			// write it back
			baos = new ByteArrayOutputStream();
			// FST
			fo = new FSTObjectOutput(baos);
			fo.writeObject(input);
			fo.close();
			byte[] retArr = baos.toByteArray();
			return retArr;
		} catch (IOException e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(fo != null) {
				try {
					fo.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
			if(baos != null) {
				try {
					baos.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
		return null;		
	}

	public static Object deserialize(byte[] data) {
		ByteArrayInputStream bais = null;
		FSTObjectInput fi = null;
		try {
			bais = new ByteArrayInputStream(data);
			fi = new FSTObjectInput(bais);
			Object object = fi.readObject();
			return object;
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(fi != null) {
				try {
					fi.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
			if(bais != null) {
				try {
					bais.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
		return null;
	}

	public static byte[] packBytes(Object obj) {
		byte[] psBytes = FstUtil.serialize(obj);

		if(psBytes == null)
			return psBytes;
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
