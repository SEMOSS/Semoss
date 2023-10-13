package prerna.reactor.expression;

import java.util.List;

import prerna.reactor.JavaExecutable;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class IfError extends OpBasic {

	@Override
	protected NounMetadata evaluate(Object[] values) {
		return this.nouns[0];
	}
	
	//TODO: implement this, just returning 1 for now
	public String getJavaSignature() {
		
		StringBuilder javaSignature = new StringBuilder();
		List<NounMetadata> inputs = getJavaInputs();
		NounMetadata tryInput = inputs.get(0);
		
		
		Object tryObj = tryInput.getValue();
		String tryString;
		
		if(tryObj instanceof JavaExecutable) {
			tryString = ((JavaExecutable)tryObj).getJavaSignature();
		} else if(tryInput.getNounType() == PixelDataType.CONST_DECIMAL) {
			tryString = tryObj.toString();
		} else if(tryInput.getNounType() == PixelDataType.CONST_STRING) {
			tryString = "\""+tryObj.toString()+"\"";
		} else {
			tryString = tryObj.toString();
		}
		
		NounMetadata defaultInput = inputs.get(1);
		Object defaultObj = tryInput.getValue();
		String defaultString;
		
		if(defaultObj instanceof JavaExecutable) {
			defaultString = ((JavaExecutable)tryObj).getJavaSignature();
			
		} else if(defaultInput.getNounType() == PixelDataType.CONST_DECIMAL) {
			
		} else if(defaultInput.getNounType() == PixelDataType.CONST_STRING) {
			
		} else {
			
		}
		
		
//		return javaSignature.toString();
		return "1";
	}
	
	@Override
	public String getReturnType() {
		return "double";
	}
}
