package prerna.reactor.frame.sql;

import java.util.HashMap;
import java.util.Map;

import prerna.query.parsers.GenExpressionWrapper;
import prerna.query.parsers.SqlParser2;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class ParseSQL2Wrapper extends AbstractReactor {

	// fairly straight forward.. parses it
	// stores the wrapper in the insight
	// creates a json structure
	// query : full query
	// paramMap : map of param names to the replacement
	
	// main reactors needed
	// parse
	// replace param
	// get param default values
	// generate query
	
	public ParseSQL2Wrapper() {
		this.keysToGet = new String[] {ReactorKeysEnum.SQL.getKey()};
		this.keyRequired = new int[] {1};
	}
	
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		
		try {
			String sql = Utility.decodeURIComponent(keyValue.get(keysToGet[0]));
			
			SqlParser2 sqlParser = new SqlParser2();
			sqlParser.parameterize = true;
			GenExpressionWrapper wrapper = sqlParser.processQuery(sql);
			
			Map <String, Object> returnMap = new HashMap<String, Object>();
			returnMap.put("query", sql);
			
			// generate the sql
			String generatedSQL = wrapper.generateQuery(false);
			generatedSQL = generatedSQL.replace("\n", " ").trim();

			returnMap.put("generated_query", generatedSQL);
			
			// get the param string list to embed
			returnMap.put("params", wrapper.getAllParamNames());
			
			String id = this.insight.setSQLWrapper(sql, wrapper);
			returnMap.put("ID", id);
			
			return new NounMetadata(returnMap, PixelDataType.MAP);
		} catch (Exception e) {
			return NounMetadata.getErrorNounMessage(e.getLocalizedMessage());
		}
	}

}
