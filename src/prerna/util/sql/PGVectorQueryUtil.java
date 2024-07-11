package prerna.util.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringEscapeUtils;

public class PGVectorQueryUtil extends PostgresQueryUtil {

	public PGVectorQueryUtil() {
		super();
	}

	public PGVectorQueryUtil(String connectionUrl, String username, String password) {
		super();
	}

	public String createEmbeddingsTable(String table) {
		return "CREATE TABLE IF NOT EXISTS "+table+"("
				+ "ID SERIAL PRIMARY KEY, "
				+ "EMBEDDING vector, "
				+ "SOURCE text, "
				+ "MODALITY text, "
				+ "DIVIDER text, "
				+ "PART text, "
				+ "TOKENS text, "
				+ "CONTENT text "
				+ ");";
	}

	public String addVectorExtension() {
		return "CREATE EXTENSION IF NOT EXISTS vector;";
	}

	public String insertEmbeddings(Connection con, String tableName, Map<Integer, Map<String, Object>> data) {
		Map<String, Object> rowData = data.get(0);
		Map<String, String> row = (Map<String, String>) rowData.get("data");

		String columnString = "embedding";
		for (String column : row.keySet()) {
			columnString += ", " + column + "";
		}
		
		String insertString = "";
		int colNum = row.keySet().size();
		for (int i = 0; i<colNum; i++) {
			insertString+="?";
		}
        String sql = "INSERT INTO "+tableName+" ("+columnString+") VALUES ("+insertString+")";
        try {
			PreparedStatement statement = con.prepareStatement(sql);

		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
        
		String valueString = "";
		for (int i = 0; i < data.size(); i++) {
			Map<String, Object> r = data.get(i);
			Map<String, String> e = (Map<String, String>) r.get("data");
			Object embedding = r.get("embedding");
			if (e == null || e.size() == 0 || embedding == null) //[1,2,3]
				continue;
			if (i != 0)
				valueString += ",";
			String tempValueHolder = "('" + embedding+"'";
			for (Entry<String, String> entry : e.entrySet()) {
				String val = entry.getValue();
				String newVal = StringEscapeUtils.escapeJava(val);
				tempValueHolder += ", '" + newVal +"'";
			}
			valueString += tempValueHolder + ")";
			if(i==data.size()-1) {
				valueString += ";";
			}
		}

		valueString += ";";

		return "INSERT INTO "+ tableName+ " (" + columnString + ") VALUES" + valueString;
	}
	
	public String removeEmbeddings(String tableName, String documentName) {
		return "DELETE from \""+tableName+"\" where \"source\"='"+documentName+"'";
	}
}
