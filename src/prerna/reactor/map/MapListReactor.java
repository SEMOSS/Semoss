package prerna.reactor.map;

import java.util.List;
import java.util.Vector;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class MapListReactor extends AbstractMapReactor {

	// this could come in here as a simple vector
	// or it could come in here as a hashtable
	// need to see which one to instantiate
	
	// I also need to see the parent reactor
	// what it is and appropriately add the stuff
	
	@Override
	public NounMetadata execute() {
		
		// couple of different cases here
		// if Hashtable property is set to true
		// then this needs to be processed as hash
		// once you get the hash
		// you need to add it to the parent
		
		// you could add it with the curnoun I bet 
		NounMetadata noun = null;
		
		//if(!this.propStore.containsKey("Hashtable"))
		{
			
			List<Object> returnValues = new Vector<Object>();
			int size = this.curRow.size();
			for(int i = 0; i < size; i++) {
				returnValues.add(getValue(this.curRow.getNoun(i)));
			}
			noun = new NounMetadata(returnValues, PixelDataType.VECTOR);
		}

		return noun;
	}

}
