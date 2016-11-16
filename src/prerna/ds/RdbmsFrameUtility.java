package prerna.ds;

public class RdbmsFrameUtility {

	public static String cleanInstance(String value) {
		return value.replace("'", "''");
	}
	
	protected static String cleanHeader(String header) {
		header = header.replaceAll("[#%!&()@#$'./-]*\"*", ""); // replace all the useless shit in one go
		header = header.replaceAll("\\s+", "_");
		header = header.replaceAll(",", "_");
		if (Character.isDigit(header.charAt(0)))
			header = "c_" + header;
		return header;
	}
}
