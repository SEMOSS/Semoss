package prerna.sablecc2.reactor;

import java.util.ArrayList;
import java.util.List;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;

public class VectorReactor extends AbstractReactor {

	@Override
	public void In() {
		curNoun("all");
	}

	@Override
	public Object Out() {
		return parentReactor;
	}
	
	public NounMetadata execute() {
		List<NounMetadata> list = new ArrayList<>();
		for(int i = 0; i < curRow.size(); i++) {
			NounMetadata noun = curRow.getNoun(i);
			list.add(noun);
		}
		return new NounMetadata(list, PkslDataTypes.VECTOR);
	}
}
