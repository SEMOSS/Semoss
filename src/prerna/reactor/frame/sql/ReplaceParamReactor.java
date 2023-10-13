package prerna.reactor.frame.sql;

import java.util.HashMap;
import java.util.Map;

import prerna.query.parsers.GenExpressionWrapper;
import prerna.query.parsers.SqlParser2;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ReplaceParamReactor extends AbstractReactor {

	public ReplaceParamReactor()
	{
		this.keysToGet = new String[] {ReactorKeysEnum.SQL.getKey(), ReactorKeysEnum.PARAM_KEY.getKey(), ReactorKeysEnum.VALUE.getKey()};
		this.keyRequired = new int[] {1, 1, 1};
	}

	
	@Override
	public NounMetadata execute() {
		// TODO Auto-generated method stub
		
		// replace the parameter
		// fill it 
		// generate the sql
		// generate wrapper from it
		// parameterize and regenerate - need to know if front end needs it like that
		organizeKeys();
		
		try
		{
			String id = keyValue.get(keysToGet[0]); // this is really the id
			String param = keyValue.get(keysToGet[1]);
			String value = keyValue.get(keysToGet[2]);
			
			GenExpressionWrapper wrapper = this.insight.getSQLWrapper(id);
			if(wrapper.setCurrentValueOfParam(param, value))
			{
				SqlParser2 sqlParser = new SqlParser2();
				
				Map <String, Object> returnMap = new HashMap<String, Object>();
				// generate the sql
				wrapper.fillParameters();
				// this will be the new sql
				String newSql = sqlParser.generateQuery(wrapper.root);
				newSql = newSql.replace("\n", " ").trim();
				returnMap.put("query", newSql);
				
				sqlParser.parameterize = true;
				wrapper = sqlParser.processQuery(newSql);
				String generatedSQL = wrapper.generateQuery(false);
				returnMap.put("generated_query", generatedSQL);
				
				// get the param string list to embed
				returnMap.put("params", wrapper.getAllParamNames());
				
				// remove the old one
				this.insight.replaceWrapper(id, newSql, wrapper);
				
				return new NounMetadata(returnMap, PixelDataType.MAP);
			}
			else
				return NounMetadata.getErrorNounMessage("No such parameter found");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			return NounMetadata.getErrorNounMessage(e.getLocalizedMessage());
		}
	}

}
