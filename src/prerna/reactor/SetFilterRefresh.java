package prerna.reactor;

import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SetFilterRefresh extends AbstractReactor {
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		boolean filterRefresh = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[0]));
		return this.insight.setInsightFilterRefresh(filterRefresh);
	}

}
