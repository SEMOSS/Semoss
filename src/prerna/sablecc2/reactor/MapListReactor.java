package prerna.sablecc2.reactor;

import java.util.List;
import java.util.Vector;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;

public class MapListReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		List<Object> returnValues = new Vector<Object>();
		int size = this.curRow.size();
		for(int i = 0; i < size; i++) {
			returnValues.add(this.curRow.get(i));
		}
		
		NounMetadata noun = new NounMetadata(returnValues, PkslDataTypes.VECTOR);
		return noun;
	}

}
