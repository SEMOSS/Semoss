package prerna.ds.py;

public class PandasSyntaxHelper {

	private PandasSyntaxHelper() {
		
	}

	/**
	 * Execute a .py file
	 * @param fileLocation
	 * @return
	 */
	public static String execFile(String fileLocation) {
		return "execfile('" + fileLocation.replaceAll("\\\\+", "/") + "')";
	}
	
	/**
	 * Get the syntax to load a csv file
	 * @param fileLocation
	 * @param tableName
	 * @param tableName2 
	 */
	public static String getFileRead(String pandasImportVar, String fileLocation, String tableName) {
		String readCsv = tableName + " = " + pandasImportVar + ".read_csv('" + fileLocation.replaceAll("\\\\+", "/") + "')";
		return readCsv;
	}

} 
