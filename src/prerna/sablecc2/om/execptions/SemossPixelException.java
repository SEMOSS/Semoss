package prerna.sablecc2.om.execptions;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SemossPixelException extends RuntimeException implements ISemossException {

	protected boolean continueThreadOfExecution = true;
	protected NounMetadata noun = null;
	protected String message = null;
	
	public SemossPixelException(String message) {
		super(message);
		this.noun = new NounMetadata(message, PixelDataType.CONST_STRING, PixelOperationType.ERROR);
	}
	
	public SemossPixelException(String message, Throwable e) {
		super(message, e);
		this.noun = new NounMetadata(message, PixelDataType.CONST_STRING, PixelOperationType.ERROR);
	}
	
    public SemossPixelException(Throwable cause) {
        super(cause);
		this.noun = new NounMetadata(cause.getMessage(), PixelDataType.CONST_STRING, PixelOperationType.ERROR);
    }
	
	public SemossPixelException(String message, boolean continueThreadOfExecution) {
		super(message);
		this.noun = new NounMetadata(message, PixelDataType.CONST_STRING, PixelOperationType.ERROR);
		this.continueThreadOfExecution = continueThreadOfExecution;
	}
	
	public SemossPixelException(NounMetadata noun) {
		this.noun = noun;
		if(this.noun.getNounType() == PixelDataType.CONST_STRING
				|| this.noun.getNounType() == PixelDataType.ERROR) {
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
