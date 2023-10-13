package prerna.reactor.frame.sql;

import java.util.HashMap;
import java.util.Map;

import prerna.query.parsers.GenExpressionWrapper;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetParamValuesReactor extends AbstractReactor {

	public GetParamValuesReactor()
	{
		this.keysToGet = new String[] {ReactorKeysEnum.SQL.getKey(), ReactorKeysEnum.PARAM_KEY.getKey()};
		this.keyRequired = new int[] {1, 1};
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
			String id = keyValue.get(keysToGet[0]); // this is the id
			String param = keyValue.get(keysToGet[1]);
			
			GenExpressionWrapper wrapper = this.insight.getSQLWrapper(id);
			String defQuery = wrapper.getQueryForParam(param);

			Map <String, Object> returnMap = new HashMap<String, Object>();
			returnMap.put("query", id);
			
			returnMap.put("params", wrapper.getAllParamNames());
			
			returnMap.put(param, defQuery);

			return new NounMetadata(returnMap, PixelDataType.MAP);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			return NounMetadata.getErrorNounMessage(e.getLocalizedMessage());
		}
	}

}
