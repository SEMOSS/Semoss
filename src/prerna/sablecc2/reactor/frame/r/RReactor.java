package prerna.sablecc2.reactor.frame.r;

import org.apache.log4j.Logger;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.util.Utility;

public class RReactor extends AbstractReactor {
	
	private static final String CLASS_NAME = RReactor.class.getName();
	
	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		AbstractRJavaTranslator rJavaTranslator = this.insight.getRJavaTranslator(logger);
		rJavaTranslator.startR();
		
		String code = Utility.decodeURIComponent(this.curRow.get(0).toString());
		logger.info("Execution r script: " + code);
		String output = rJavaTranslator.runRAndReturnOutput(code);
		return new NounMetadata(output, PixelDataType.CONST_STRING);
	}

}
