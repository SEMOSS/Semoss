package prerna.sablecc2.reactor;

import java.util.List;

import prerna.sablecc2.om.NounMetadata;

public interface JavaExecutable {

	public String getJavaSignature();
	
	public List<NounMetadata> getJavaInputs();
	
	public String getReturnType();
}
