package prerna.sablecc2.reactor;

import prerna.sablecc2.om.nounmeta.NounMetadata;

public class EmbeddedScriptReactor extends AbstractReactor {
	
	// this class does nothing!
	// it is meant when we have an embedded script 
	// within our main script
	// but i just want to push the output of this to the main script
	
	@Override
	public NounMetadata execute() {
		int size = this.curRow.size();
		return this.curRow.getNoun(size - 1);
	}
}