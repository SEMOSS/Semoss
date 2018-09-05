package prerna.util.usertracking.reactors;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class WidgetTrackingReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		for(String key : this.store.getNounKeys()) {
			GenRowStruct grs = this.store.getNoun(key);
			System.out.println(grs.getAllValues());
		}
		return null;
	}

}
