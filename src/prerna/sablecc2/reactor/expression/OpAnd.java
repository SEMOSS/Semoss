package prerna.sablecc2.reactor.expression;

import java.util.List;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.reactor.JavaExecutable;

public class OpAnd extends OpBasic {

	@Override
	protected NounMetadata evaluate(Object[] values) {
		boolean result = eval(values);
		return new NounMetadata(result, PkslDataTypes.BOOLEAN);
	}
	
	public static boolean eval(Object...values) {
		boolean result = true;
		for (Object booleanValue : values) {
			// need all values to be true
			// in order to return true
			if(! (boolean) booleanValue) {
				result = false;
				break;
			}
		}
		return result;
	}
	
	public static boolean eval(boolean[] values) {
		boolean result = true;
		for (Object booleanValue : values) {
			// need all values to be true
			// in order to return true
			if(! (boolean) booleanValue) {
				result = false;
				break;
			}
		}
		return result;
	}

	@Override
	public String getJavaSignature() {
		StringBuilder javaSignature = new StringBuilder(this.getClass().getName()+".eval(new boolean[] {");
		List<NounMetadata> inputs = this.getJavaInputs();
		for(int i = 0; i < inputs.size(); i++) {
			if(i > 0) {
				javaSignature.append(", ");
			}
			
			String nextArgument;
			NounMetadata nextNoun = inputs.get(i);
			Object nextInput = inputs.get(i).getValue();
			if(nextInput instanceof JavaExecutable) {
				nextArgument = ((JavaExecutable)nextInput).getJavaSignature();
			} else {
				if(nextNoun.getNounType() == PkslDataTypes.CONST_STRING) {
					nextArgument = "\""+nextInput.toString() +"\"";
				} else {
					nextArgument = nextInput.toString();
				}
			}
			javaSignature.append(nextArgument);
		}
		javaSignature.append("})");
		
		return javaSignature.toString();
	}
	
	
	@Override
	public String getReturnType() {
		return "boolean";
	}
}
