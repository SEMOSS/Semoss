package prerna.reactor.map;

import java.util.HashMap;
import java.util.Map;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class MapReactor extends AbstractMapReactor {
	
	@Override
	public NounMetadata execute() {
		Map<Object, Object> mapObj = new HashMap<Object, Object>();
		// the map must have a key and value pairs
		// we we will use this to determine each key/value that we need to create
		int size = this.curRow.size();
		for(int i = 0; i < size; i=i+2) {
			mapObj.put(getValue(this.curRow.getNoun(i)), getValue(this.curRow.getNoun(i+1)));
		}
		
		NounMetadata noun = new NounMetadata(mapObj, PixelDataType.MAP);
		return noun;
	}

}
