package prerna.ds.rdbms.h2;

public class H2ArrayToStringFunction {

//	EXAMPLE FOR ADDING IN H2 CONSOLE
//	
//	CREATE ALIAS ARRAY_TO_STRING AS $$
//	String turnArrayToString(Object[] array, String separator) {
//			if(array == null || array.length == 0) {
//				return "";
//			}
//			
//			StringBuilder b = new StringBuilder();
//			b.append(array[0]);
//			for(int i = 1; i < array.length; i++) {
//				b.append(separator).append(array[i]);
//			}
//			return b.toString();
//		}
//	$$;
	
	public static String turnArrayToString(Object[] array, String separator) {
		if(array == null || array.length == 0) {
			return "";
		}
		
		StringBuilder b = new StringBuilder();
		b.append(array[0]);
		for(int i = 1; i < array.length; i++) {
			b.append(separator).append(array[i]);
		}
		return b.toString();
	}
	
}