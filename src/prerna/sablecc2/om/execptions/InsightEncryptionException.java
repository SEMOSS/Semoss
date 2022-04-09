package prerna.sablecc2.om.execptions;

import prerna.sablecc2.om.nounmeta.NounMetadata;

public class InsightEncryptionException extends SemossPixelException {

	public InsightEncryptionException(String message) {
		super(message);
	}
	
	public InsightEncryptionException(String message, boolean continueThreadOfExecution) {
		super(message);
	}
	
	public InsightEncryptionException(NounMetadata noun) {
		super(noun);
	}
	
}
