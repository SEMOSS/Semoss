package prerna.sablecc2.reactor.frame.r;

import org.apache.log4j.Logger;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.frame.r.util.AbstractRJavaTranslator;

public class CancelRReactor extends AbstractReactor {
	
	private static final String CLASS_NAME = CancelRReactor.class.getName();
	
	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		AbstractRJavaTranslator rJavaTranslator = this.insight.getRJavaTranslator(logger);		
		rJavaTranslator.startR();
		boolean cancelled = rJavaTranslator.cancelExecution();
		return new NounMetadata(cancelled, PixelDataType.BOOLEAN);
	}
}
