package prerna.sablecc2.reactor.imports;

import prerna.om.Insight;

public abstract class AbstractImporter implements IImporter {

	protected Insight in = null;
	
	@Override
	public void setInsight(Insight in) {
		this.in = in;
	}

}
