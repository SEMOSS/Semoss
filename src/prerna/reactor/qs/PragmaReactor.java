package prerna.reactor.qs;

import java.util.List;
import java.util.Map;

import prerna.query.querystruct.AbstractQueryStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;

public class PragmaReactor extends AbstractQueryStructReactor {
	
	// specifies pragmas with this query
	// it could be things like override
	// or 
	
	public PragmaReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PRAGMA.getKey()};
	}

	protected AbstractQueryStruct createQueryStruct() {
		List<Object> mapOptions = this.curRow.getValuesOfType(PixelDataType.MAP);
		
		// usually there should be just one
		// and that does it
		// I am in dual mind whether to use this to override or have a separate reactor
		// I think this should logically sit in the insight and then get passed
		if(mapOptions != null && mapOptions.size() > 0) {
			// if it is null, i guess we just clear the map values
			//this.qs.setPragmap(mapOptions);
			// see if this also has a key
			// specified by type and if so set it in the appropriate place
			// if no key this will go into the insight
			String type = "insight";
			Map thisMap =(Map <String, Object>) mapOptions.get(0);
			if(thisMap.containsKey("type"))
				type = (String)thisMap.get("type");
			
			if(type.equalsIgnoreCase("insight") && this.qs.getSelectors().size() == 0) // also it is replacing it as opposed to adding it // need a way to identify when it was not set up that way. May be I just do the execute here instead of abstract querystruct
			{
				if(thisMap.size() > 0 && this.insight.getPragmap() != null ) // append it
					this.insight.getPragmap().putAll(thisMap);
				else if(this.insight.getPragmap() == null)
					this.insight.setPragmap(thisMap);
				else // the user is trying to clear it
					this.insight.clearPragmap();
			}
			else // this can only be used enroute in a query - we can change it so that you can change it after the fact. I just dont feel that comfortable with it
				this.qs.setPragmap((Map<String, Object>) mapOptions.get(0));
		}
		return qs;
	}
}
