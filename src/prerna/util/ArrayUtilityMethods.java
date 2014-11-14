package prerna.util;

import java.util.Arrays;

public final class ArrayUtilityMethods {

	private static final String ERROR = "The data array either is null or does not contain any data.";
	
	private ArrayUtilityMethods() {
		
	}
	
	public static int calculateIndexOfArray(final String[] arr, final String value) {
		if(arr == null) {
			throw new NullPointerException(ERROR);
		}
		int size = arr.length;
		int index;
		for(index = 0; index < size; index++) {
			if(arr[index] != null && arr[index].equals(value)) {
				return index;
			}
		}
		return -1;
	}

	public static boolean arrayContainsValue(final Object[] arr, final Object value) {
		if(arr == null) {
			throw new NullPointerException(ERROR);
		}
		
		int size = arr.length;
		int index;
		for(index = 0; index < size; index++) {
			if(arr[index] != null && arr[index] == value) {
				return true;
			}
		}
		return false;
	}
	
	public static boolean arrayContainsValue(final String[] arr, final String value) {
		if(arr == null) {
			throw new NullPointerException(ERROR);
		}
		int size = arr.length;
		int index;
		for(index = 0; index < size; index++) {
			if(arr[index] != null && arr[index].equals(value)) {
				return true;
			}
		}
		return false;
	}
	
public static boolean arrayContainsValue(final double[] arr, final double value) {
		
		if(arr == null) {
			throw new NullPointerException(ERROR);
		}
		
		int size = arr.length;
		if(size == 0) {
			return false; //empty array
		}
		
		int index;
		for(index = 0; index < size; index++) {
			if(arr[index] == value) {
				return true;
			}
		}
		return false;
	}
	
	public static boolean arrayContainsValue(final int[] arr, final int value) {
		
		if(arr == null) {
			throw new NullPointerException(ERROR);
		}
		
		int size = arr.length;
		if(size == 0) {
			return false; //empty array
		}
		
		int index;
		for(index = 0; index < size; index++) {
			if(arr[index] == value) {
				return true;
			}
		}
		return false;
	}

	public static int[] findAllClosestNonNullIndex(Integer[] arr, int index) {
		if(removeAllNulls(arr).length == 0) {
			throw new NullPointerException(ERROR);
		}
		int[] minDiff = new int[]{Integer.MAX_VALUE};
		
		int i;
		int size = arr.length;
		for(i = 0; i < size; i++) {
			if(arr[i] != null) {
				int localDiff = Math.abs(index - i);
				if(localDiff < minDiff[0]) {
					minDiff[0] = i;
				} else if(localDiff == minDiff[0]) {
					int firstIdx = minDiff[0];
					minDiff = new int[]{firstIdx, i};
				}
			}
		}
		
		return minDiff;
	}
	
	public static int[] findAllClosestNonNullIndex(Double[] arr, int index) {
		if(removeAllNulls(arr).length == 0) {
			throw new NullPointerException(ERROR);
		}
		int[] minIndex = new int[1];
		int minDiff = Integer.MAX_VALUE;
		int i;
		int size = arr.length;
		for(i = 0; i < size; i++) {
			if(arr[i] != null) {
				int localDiff = Math.abs(index - i);
				if(localDiff < minDiff) {
					minDiff = localDiff;
					minIndex[0] = i;
				} else if(localDiff == minDiff) {
					int firstIdx = minIndex[0];
					minIndex = new int[]{firstIdx, i};
				}
			}
		}
		
		return minIndex;
	}

	public static int determineLastNonNullValue(final Object[] arr) {
		int lastNonNullValue = -1;
		int size = arr.length;
		int index;
		for(index = 0; index < size; index++) {
			if(arr[index] == null) {
				continue;
			} else {
				lastNonNullValue = index;
			}
		}
		return lastNonNullValue;
	}
	
	public static Object[] trimEmptyValues(final Object[] arr) {
		int lastNonNullValue = determineLastNonNullValue(arr);
		if(lastNonNullValue == -1) {
			return null;
		}
		return Arrays.copyOfRange(arr, 0, lastNonNullValue+1);
	}

	public static Object[] removeAllNulls(final Object[] arr) {
		int positionInOriginalArr;
		int positionInNewArr;
		int originalLength = positionInOriginalArr = positionInNewArr = arr.length;
		while (positionInOriginalArr > 0) {
			Object value = arr[--positionInOriginalArr];
			if (value != null) {
				arr[--positionInNewArr] = value;
			}
		}
		return Arrays.copyOfRange(arr, positionInNewArr, originalLength);
	}
	
	public static double[] removeAllZeroValues(final double[] arr) {
		int positionInOriginalArr;
		int positionInNewArr;
		int originalLength = positionInOriginalArr = positionInNewArr = arr.length;
		while (positionInOriginalArr > 0) {
			double value = arr[--positionInOriginalArr];
			if (value != 0) {
				arr[--positionInNewArr] = value;
			}
		}
		return Arrays.copyOfRange(arr, positionInNewArr, originalLength);
	}
	
	public static int[] removeAllZeroValues(final int[] arr) {
		int positionInOriginalArr;
		int positionInNewArr;
		int originalLength = positionInOriginalArr = positionInNewArr = arr.length;
		while (positionInOriginalArr > 0) {
			int value = arr[--positionInOriginalArr];
			if (value != 0) {
				arr[--positionInNewArr] = value;
			}
		}
		return Arrays.copyOfRange(arr, positionInNewArr, originalLength);
	}

	public static double[] removeAllTrailingZeroValues(final double[] arr) {
		int newSize = arr.length;
		while(newSize-- > 0 && arr[newSize] != 0) {};
		double[] retArr = new double[newSize+1];
		System.arraycopy(arr, 0, retArr, 0, newSize+1);
		
		return retArr;
	}
	
	public static int[] removeAllTrailingZeroValues(final int[] arr) {
		int newSize = arr.length;
		// check to make sure there are trailing zeros to remove
		if(arr[newSize - 1] != 0) {
			return arr;
		}
		// check if all values are zero, return empty array
		if(removeAllZeroValues(arr.clone()).length == 0) {
			return new int[]{};
		}
		
		while(newSize - 1 > 0 && arr[newSize - 1] == 0) {
			newSize--;
		};
		int[] retArr = new int[newSize];
		System.arraycopy(arr, 0, retArr, 0, newSize);
		
		return retArr;
	}
	
	public static Object[] resizeArray(final Object[] arr, final int factor) {
		return Arrays.copyOf(arr, arr.length*2); 
	}
	
	public static int[] resizeArray(final int[] arr, int factor) {
		int i;
		int size = arr.length;
		int[] retArr = new int[size*2];
		for(i = 0; i < size; i++) {
			retArr[i] = arr[i];
		}
		return retArr; 
	}
	
	public static double[] resizeArray(final double[] arr, int factor) {
		int i;
		int size = arr.length;
		double[] retArr = new double[size*2];
		for(i = 0; i < size; i++) {
			retArr[i] = arr[i];
		}
		return retArr; 
	}
	
	public static double[] convertObjArrToDoubleArr(final Object[] arr) {
		int size = arr.length;
		double[] retArr = new double[size];
		int index;
		for(index = 0; index < size; index++) {
			Object obj = arr[index];
			double val = 0;
			try {
				val = Double.valueOf(obj.toString());
			} catch(NumberFormatException ex) {
				throw new NumberFormatException("Value in Object array cannot be converted to double");
			}
			retArr[index] = val;
		}
		return retArr;
	}
	
	public static Double[] convertObjArrToDoubleWrapperArr(final Object[] arr) {
		int size = arr.length;
		Double[] retArr = new Double[size];
		int index;
		for(index = 0; index < size; index++) {
			Object obj = arr[index];
			Double val = 0.0;
			if(obj != null) {
				try {
					val = Double.valueOf(obj.toString());
				} catch(NumberFormatException ex) {
					throw new NumberFormatException("Value in Object array cannot be converted to double");
				}
				retArr[index] = val;
			}
		}
		return retArr;
	}
	
	public static String[] convertObjArrToStringArr(final Object[] arr) {
		int size = arr.length;
		String[] retArr = new String[size];
		int index;
		for(index = 0; index < size; index++) {
			Object obj = arr[index];
			String val = null;
			if(obj != null) {
				val = obj.toString();
			}
			retArr[index] = val;
		}
		return retArr;
	}
	
	public static String[] convertDoubleArrToStringArr(final double[] arr) {
		int size = arr.length;
		String[] retArr = new String[size];
		int index;
		for(index = 0; index < size; index++) {
			double dObj = arr[index];
			retArr[index] = dObj + "";
		}
		return retArr;
	}
	
	public static String[] getUniqueArray(final String[] arr) {
        int size = arr.length;
        String[] temp = new String[size];

		int counter = 0;
        int index;
        for (index = 0; index < size; index++) {
            if(!arrayContainsValue(temp, arr[index])) {
                temp[counter++] = arr[index];
            }
        }
        String[] uniqueArray = new String[counter];
        System.arraycopy(temp, 0, uniqueArray, 0, uniqueArray.length);
 
        return uniqueArray;
    }
	
	public static double[] getUniqueArray(final double[] arr) {
        int size = arr.length;
        double[] temp = new double[size];

		int counter = 0;
        int index;
        for (index = 0; index < size; index++) {
            if(!arrayContainsValue(temp, arr[index])) {
                temp[counter++] = arr[index];
            }
        }
        double[] uniqueArray = new double[counter];
        System.arraycopy(temp, 0, uniqueArray, 0, uniqueArray.length);
 
        return uniqueArray;
    }
}
