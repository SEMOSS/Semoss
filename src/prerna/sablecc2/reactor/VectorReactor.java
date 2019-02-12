package prerna.sablecc2.reactor;

import java.util.ArrayList;
import java.util.List;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class VectorReactor extends AbstractReactor implements JavaExecutable {

	public NounMetadata execute() {
		List<NounMetadata> list = new ArrayList<>();
		for(int i = 0; i < curRow.size(); i++) {
			NounMetadata noun = curRow.getNoun(i);
			list.add(noun);
		}
	
		return new NounMetadata(list, PixelDataType.VECTOR, PixelOperationType.VECTOR);
	}

	public String getJavaSignature() {
		StringBuilder javaSignature = new StringBuilder("new Object[] {");
		List<NounMetadata> inputs = this.getJavaInputs();
		for(int i = 0; i < inputs.size(); i++) {
			if(i > 0) {
				javaSignature.append(", ");
			}
			
			String nextArgument;
			NounMetadata nextNoun = inputs.get(i);
			Object nextInput = nextNoun.getValue();
			if(nextInput instanceof JavaExecutable) {
				nextArgument = ((JavaExecutable)nextInput).getJavaSignature();
			} else {
				if(nextNoun.getNounType() == PixelDataType.CONST_STRING) {
					nextArgument = "\""+nextInput.toString() +"\"";
				} else {
					nextArgument = nextInput.toString();
				}
			}
			javaSignature.append(nextArgument);
		}
		javaSignature.append("}");
		
		return javaSignature.toString();
	}
	
	public List<NounMetadata> getJavaInputs() {
		List<NounMetadata> list = new ArrayList<>();
		for(int i = 0; i < curRow.size(); i++) {
			NounMetadata noun = curRow.getNoun(i);
			list.add(noun);
		}
		return list;
	}
	
	@Override
	public String getReturnType() {
		return "Object[]";
	}
}
