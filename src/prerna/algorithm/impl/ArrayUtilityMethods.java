package prerna.algorithm.impl;

import java.util.Arrays;

import org.apache.lucene.util.ArrayUtil;

public class ArrayUtilityMethods {

	public static int calculateIndexOfArray(String[] arr, String value) {
		int size = arr.length;
		int i;
		for(i = 0; i < size; i++) {
			if(arr[i] != null && arr[i].equals(value)) {
				return i;
			}
		}
		return -1;
	}

	public static boolean arrayContainsValue(Object[] arr, Object value) {
		if(arr == null) {
			return false;
		}
		int size = arr.length;
		int i;
		for(i = 0; i < size; i++) {
			if(arr[i] != null && arr[i] == value) {
				return true;
			}
		}
		return false;
	}
	
	public static boolean arrayContainsValue(String[] arr, String value) {
		if(arr == null) {
			return false;
		}
		int size = arr.length;
		int i;
		for(i = 0; i < size; i++) {
			if(arr[i] != null && arr[i].equals(value)) {
				return true;
			}
		}
		return false;
	}

	public static int determineLastNonNullValue(Object[] arr) {
		int lastNonNullValue = 0;
		int size = arr.length;
		int i;
		for(i = 0; i < size; i++) {
			if(arr[i] == null) {
				continue;
			} else {
				lastNonNullValue = i;
			}
		}
		return lastNonNullValue;
	}
	
	public static Object[] trimEmptyValues(Object[] arr) {
		int lastNonNullValue = determineLastNonNullValue(arr);
		if(lastNonNullValue == 0) {
			return null;
		}
		Object[] returnArr = Arrays.copyOfRange(arr, 0, lastNonNullValue+1);
		return returnArr;
	}

	public static boolean arrayContainsValue(int[] arr, int value) {
		
		if(arr == null) {
			return false;
		}
		
		int size = arr.length;
		if(size == 0) {
			return false; //empty array
		}
		
		int i;
		for(i = 0; i < size; i++) {
			if(arr[i] == value) {
				return true;
			}
		}
		return false;
	}

	public static Object[] removeAllNulls(Object[] arr) {
		int positionInOriginalArr;
		int positionInNewArr;
		int originalLength = positionInOriginalArr = positionInNewArr = arr.length;
		while (positionInOriginalArr > 0) {
			Object o = arr[--positionInOriginalArr];
			if (o != null) {
				arr[--positionInNewArr] = o;
			}
		}
		return Arrays.copyOfRange(arr, positionInNewArr, originalLength);
	}

	public static Object[] resizeArray(Object[] arr, int factor) {
		return Arrays.copyOf(arr, arr.length*2); 
	}
	
	public static double[] convertObjArrToDoubleArr(Object[] arr) {
		int size = arr.length;
		double[] retArr = new double[size];
		int i;
		for(i = 0; i < size; i++) {
			Object obj = arr[i];
			double val = 0;
			try {
				val = Double.valueOf(obj.toString()).doubleValue();
			} catch(NumberFormatException ex) {
				throw new NumberFormatException("Value in Object array cannot be converted to double");
			}
			retArr[i] = val;
		}
		return retArr;
	}
	
	public static String[] convertObjArrToStringArr(Object[] arr) {
		int size = arr.length;
		String[] retArr = new String[size];
		int i;
		for(i = 0; i < size; i++) {
			Object obj = arr[i];
			String val = null;
			if(obj != null) {
				val = obj.toString();
			}
			retArr[i] = val;
		}
		return retArr;
	}
	
	public static String[] getUniqueArray(String[] arr) {
        int size = arr.length;
        String[] temp = new String[size];

		int counter = 0;
        int i;
        for (i = 0; i < size; i++) {
            if(!arrayContainsValue(temp, arr[i])) {
                temp[counter++] = arr[i];
            }
        }
        String[] uniqueArray = new String[counter];
        System.arraycopy(temp, 0, uniqueArray, 0, uniqueArray.length);
 
        return uniqueArray;
    }

}
