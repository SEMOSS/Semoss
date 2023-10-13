package prerna.reactor.frame.sql;

import java.util.HashMap;
import java.util.Map;

import prerna.query.parsers.GenExpressionWrapper;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GenerateQueryReactor extends AbstractReactor {

	public GenerateQueryReactor()
	{
		this.keysToGet = new String[] {ReactorKeysEnum.SQL.getKey()};
		this.keyRequired = new int[] {1};
	}

	
	@Override
	public NounMetadata execute() {
		// TODO Auto-generated method stub
		
		organizeKeys();
		
		try
		{
			String sql = keyValue.get(keysToGet[0]);
			String param = keyValue.get(keysToGet[1]);
			
			GenExpressionWrapper wrapper = this.insight.getSQLWrapper(sql);
			wrapper.fillParameters();
			wrapper.generateQuery(true);
			
			Map <String, Object> returnMap = new HashMap<String, Object>();
			returnMap.put("query", sql);

			return new NounMetadata(returnMap, PixelDataType.MAP);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			return NounMetadata.getErrorNounMessage(e.getLocalizedMessage());
		}
	}

}
