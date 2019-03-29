package prerna.sablecc2.reactor.frame.r;

import java.util.List;
import java.util.Vector;

import org.apache.log4j.Logger;

import prerna.sablecc.ReactorSecurityManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.util.Utility;

public class RReactor extends AbstractReactor {
	
	private static transient SecurityManager defaultManager = System.getSecurityManager();
	private static final String CLASS_NAME = RReactor.class.getName();
	
	@Override
	public NounMetadata execute() {
		ReactorSecurityManager tempManager = new ReactorSecurityManager();
		tempManager.addClass(CLASS_NAME);
		System.setSecurityManager(tempManager);
		
		Logger logger = getLogger(CLASS_NAME);
		AbstractRJavaTranslator rJavaTranslator = this.insight.getRJavaTranslator(logger);
		rJavaTranslator.startR();
		
		String code = Utility.decodeURIComponent(this.curRow.get(0).toString());
		logger.info("Execution r script: " + code);
		String output = rJavaTranslator.runRAndReturnOutput(code);
		
		List<NounMetadata> outputs = new Vector<NounMetadata>(1);
		outputs.add(new NounMetadata(output, PixelDataType.CONST_STRING));
		
		// set back the original security manager
		tempManager.removeClass(CLASS_NAME);
		System.setSecurityManager(defaultManager);	
		
		return new NounMetadata(outputs, PixelDataType.CODE, PixelOperationType.CODE_EXECUTION);
	}

}
