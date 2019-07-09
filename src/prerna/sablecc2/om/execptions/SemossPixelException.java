package prerna.sablecc2.om.execptions;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SemossPixelException extends RuntimeException implements ISemossException {

	private boolean continueThreadOfExecution = true;
	private NounMetadata noun = null;
	
	private String message = null;
	
	public SemossPixelException(NounMetadata noun) {
		this.noun = noun;
		if(this.noun.getNounType() == PixelDataType.CONST_STRING) {
			this.message = this.noun.getValue() + "";
		}
	}
	
	@Override
	public boolean isContinueThreadOfExecution() {
		return this.continueThreadOfExecution;
	}

	@Override
	public void setContinueThreadOfExecution(boolean continueThreadOfExecution) {
		this.continueThreadOfExecution = continueThreadOfExecution;
	}

	@Override
	public NounMetadata getNoun() {
		return this.noun;
	}

	@Override
	public String getMessage() {
		if(this.message == null) {
			return super.getMessage();
		}
        return this.message;
	}
	
}
