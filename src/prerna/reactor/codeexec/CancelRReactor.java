package prerna.reactor.codeexec;

import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

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
