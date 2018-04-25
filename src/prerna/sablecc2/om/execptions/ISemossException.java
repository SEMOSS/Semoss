package prerna.sablecc2.om.execptions;

import prerna.sablecc2.om.nounmeta.NounMetadata;

public interface ISemossException {

	/**
	 * Get if this exception should stop the thread and return to the FE
	 * or if we should continue and try to run through the other steps
	 * @return
	 */
	boolean isContinueThreadOfExecution();
	
	/**
	 * Set if this exception should stop the thread and return to the FE
	 * or if we should continue and try to run through the other steps
	 * @return
	 */
	void setContinueThreadOfExecution(boolean continueThreadOfExecution);
	
	/**
	 * Get additional metadata to send to the FE when this exception occurs
	 * @return
	 */
	NounMetadata getNoun();
	
}
