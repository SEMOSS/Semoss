package prerna.util.sql;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.Map.Entry;

import com.amazon.redshift.core.jdbc42.PGJDBC42PreparedStatement;
import com.mysql.jdbc.Connection;
import com.pgvector.PGvector;

import edu.stanford.nlp.neural.Embedding;
import javassist.expr.NewArray;
import jnr.ffi.Struct.int16_t;

import java.util.Set;

import org.apache.commons.lang.StringEscapeUtils;
import org.json.JSONArray;

public class PGVectorQueryUtil extends PostgresQueryUtil {

	public PGVectorQueryUtil() {
		super();
	}

	public PGVectorQueryUtil(String connectionUrl, String username, String password) {
		super();
	}

	public String createEmbeddingsTable(String schema, String table) {
		return "CREATE TABLE " + schema
				+ "."+table+"( id SERIAL PRIMARY KEY, embedding vector,source text,modality text, divider text, part text, tokens text, content text );";
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

	public String isExistsEmbedding(String tableName, String documentName) {
		return "SELECT count(*) from \""+tableName+"\" where \"source\"='"+documentName+"'";
	}
	
	public String searchNearestNeighbour(String tableName, Object embedding,Number limit) {
		return "SELECT \"source\", \"modality\", \"divider\", \"part\", \"tokens\", \"content\",  1 - (\"embedding\" <=> '"+embedding+"') AS cosine_similarity FROM "+tableName+" ORDER BY cosine_similarity desc LIMIT "+limit;
	}
}
