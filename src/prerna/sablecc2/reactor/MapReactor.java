package prerna.sablecc2.reactor;

import java.util.HashMap;
import java.util.Map;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;

public class MapReactor extends AbstractReactor {
	
	@Override
	public NounMetadata execute() {
		Map<Object, Object> mapObj = new HashMap<Object, Object>();
		// the map must have a key and value pairs
		// we we will use this to determine each key/value that we need to create
		int size = this.curRow.size();
		for(int i = 0; i < size; i=i+2) {
			mapObj.put(this.curRow.get(i), this.curRow.get(i+1));
		}
		
		NounMetadata noun = new NounMetadata(mapObj, PkslDataTypes.MAP);
		return noun;
	}

}
