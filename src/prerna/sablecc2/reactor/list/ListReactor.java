package prerna.sablecc2.reactor.list;

import java.util.List;
import java.util.Vector;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class ListReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		
		// couple of different cases here
		// if Hashtable property is set to true
		// then this needs to be processed as hash
		// once you get the hash
		// you need to add it to the parent
		
		// you could add it with the curnoun I bet 
		NounMetadata noun = null;
		{
			List<NounMetadata> returnValues = new Vector<NounMetadata>();
			int size = this.curRow.size();
			for(int i = 0; i < size; i++) {
				returnValues.add(this.curRow.getNoun(i));
			}
			noun = new NounMetadata(returnValues, PixelDataType.VECTOR);
		}

		return noun;
	}
	
}
