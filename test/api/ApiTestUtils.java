package api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;

import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.gson.GsonUtility;

public class ApiTestUtils {
	
	private static final Logger LOGGER = LogManager.getLogger(ApiTestUtils.class.getName());
	
	public static String convertMapToPixelInput(Object data) {
		  Gson gson = GsonUtility.getDefaultGson();
		  return gson.toJson(data);
	}
	
	public static void print(Object data) {
		  Gson gson = GsonUtility.getDefaultGson();
		  System.out.println( gson.toJson(data));
	}
	
	public static NounMetadata processPixel(String pixel) {
		NounMetadata ret = processRawPixel(pixel);
		PixelDataType nounType = ret.getNounType();
		if (nounType == PixelDataType.ERROR || nounType == PixelDataType.INVALID_SYNTAX) {
			LOGGER.error(Constants.ERROR_MESSAGE, ret.getValue().toString());
			throw new SemossPixelException(ret.getValue().toString());
		}
		assertNotEquals(PixelDataType.ERROR, ret.getNounType());
		return ret;
		
	}
	
	public static NounMetadata processRawPixel(String pixel) {
		PixelRunner pr = new PixelRunner();
		
		try {
			pr.runPixel(pixel, ApiTests.insight);
		} catch(SemossPixelException e) {
			LOGGER.error(Constants.ERROR_MESSAGE, e);
			throw e;
		} catch(Exception e) {
			LOGGER.error(Constants.ERROR_MESSAGE, e);
			throw e;
		}
		
		NounMetadata ret = pr.getResults().get(0);
		return ret;
	}

	public static String buildPixelCall(Class<?> cl, Object... args) {
		String call = cl.getSimpleName().replace("Reactor", "");
		call += "(";
		for (int i = 0; i < args.length; i += 2) {
			if (i > 0) {
				call += ", ";
			}
			String key = args[i].toString();
			call += key;
			call += "=";
			if (key.equalsIgnoreCase("sort") || key.equalsIgnoreCase("filters")) {
				call += args[i+1].toString();
			} else {
				call += convertMapToPixelInput(args[i + 1]);
			}
		}
		call += ");";
		return call;
	}
	
	public static void checkNounMetadataError(NounMetadata nm, String errorMessage) {
		PixelOperationType nmType = nm.getOpType().get(0);
		assertEquals(PixelOperationType.ERROR, nmType);
		assertEquals(nm.getValue().toString(), errorMessage);
	}

	public static String buildFilter(String column, String comparison, String val) {
		String result = "";
		result += "[( Filter (" + column + " " + comparison + "\"" + val + "\"))]";
		return result;
	}

	public static String buildSort(String column, String dir) {
		String sort = "[ Sort ( columns = [" + column + "], sort = [" + dir + "])]";
		return sort;
	}

	public static String buildFilter(String column, String comparison, int i) {
		String result = "";
		result += "[( Filter (" + column + " " + comparison + "\"" + Integer.valueOf(i).toString() + "\"))]";
		return result;
	}

}
