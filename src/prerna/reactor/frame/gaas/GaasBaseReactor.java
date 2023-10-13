package prerna.reactor.frame.gaas;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;

public abstract class GaasBaseReactor extends AbstractReactor {

	public String getProjectId()
	{
		String projectId = null;
		
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.PROJECT.getKey());
		
		if(grs != null && !grs.isEmpty())
			projectId = grs.get(0).toString();
		else
		{
			projectId = this.insight.getProjectId();
			if(projectId == null)
				projectId = this.insight.getContextProjectId();
		}

		return projectId;
	}
	
	public Map processParamMap()
	{
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
		if(grs != null)
		{
			List maps = grs.getValuesOfType(PixelDataType.MAP);
			if(maps != null && maps.size() > 0)
				return (Map)maps.get(0);
		}
		return null;
	}
	
	public String processMapToString(Map inputMap)
	{
		StringBuilder buf = new StringBuilder("");
		Iterator <String> keys = inputMap.keySet().iterator();
		while(keys.hasNext())
		{
			String thisKey = keys.next();
			Object value = inputMap.get(thisKey);
			
			if(buf.length() != 0)
				buf.append(", ");
			
			// add the key
			buf.append(thisKey).append("=");
			
			// add the value
			if(value instanceof String)
				buf.append("\"").append(value).append("\"");
			else
				buf.append(value);
		}
		
		return buf.toString();
		
		
	}
	
}
