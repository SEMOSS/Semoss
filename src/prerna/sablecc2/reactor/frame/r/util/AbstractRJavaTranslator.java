package prerna.sablecc2.reactor.frame.r.util;

import org.apache.log4j.Logger;

import prerna.om.Insight;

public abstract class AbstractRJavaTranslator implements IRJavaTranslator {

	protected Insight insight = null;
	protected Logger logger = null;

	@Override
	public void setInsight(Insight insight) {
		this.insight = insight;
	}
	
	@Override
	public void setLogger(Logger logger) {
		this.logger = logger;
	}
	
}
