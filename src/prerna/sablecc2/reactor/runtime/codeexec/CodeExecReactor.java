package prerna.sablecc2.reactor.runtime.codeexec;

import org.apache.logging.log4j.Logger;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.util.Utility;

public class CodeExecReactor extends AbstractReactor {

	private static final String CLASS_NAME = CodeExecReactor.class.getName();
	
	public CodeExecReactor() {
		this.keysToGet = new String[]{"type", "code"};
	}
	
	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		organizeKeys();
		String type = this.keyValue.get(this.keysToGet[0]).toUpperCase();
		
		String output = null;
		String encodedCode = (String) this.propStore.get("CODE");
		String code = Utility.decodeURIComponent(encodedCode);
		
		if(type.equals("R")) {
			AbstractRJavaTranslator rJavaTranslator = this.insight.getRJavaTranslator(logger);
			rJavaTranslator.startR();
			output = rJavaTranslator.runRAndReturnOutput(code);
		} else if(type.equals("PYTHON")) {
			
		}
		
		return new NounMetadata(output, PixelDataType.CONST_STRING, PixelOperationType.CODE_EXECUTION);
	}

	
	
}
