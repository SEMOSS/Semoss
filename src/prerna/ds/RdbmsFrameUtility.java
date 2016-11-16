package prerna.ds;

public class RdbmsFrameUtility {

	public static String cleanInstance(String value) {
		return value.replace("'", "''");
	}
}
