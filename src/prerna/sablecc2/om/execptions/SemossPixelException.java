package prerna.sablecc2.om.execptions;

import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SemossPixelException extends RuntimeException implements ISemossException{

	private boolean continueThreadOfExecution = true;
	private NounMetadata noun = null;
	
	@Override
	public boolean isContinueThreadOfExecution() {
		return this.continueThreadOfExecution;
	}

	@Override
	public void setContinueThreadOfExecution(boolean continueThreadOfExecution) {
		this.continueThreadOfExecution = continueThreadOfExecution;
	}

	@Override
	public NounMetadata getAdditionalReturn() {
		return this.noun;
	}

	@Override
	public void setAdditionalReturn(NounMetadata noun) {
		this.noun = noun;
	}

}
