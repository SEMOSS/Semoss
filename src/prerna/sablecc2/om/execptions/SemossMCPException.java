package prerna.sablecc2.om.execptions;

import prerna.reactor.agent.mcp.MCPErrorCode;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SemossMCPException extends SemossPixelException {

	private MCPErrorCode error = null;
	
	public SemossMCPException(String message, MCPErrorCode error) {
		super(message);
		this.error = error;
	}
	
	public SemossMCPException(String message, Throwable e, MCPErrorCode error) {
		super(message, e);
		this.error = error;
	}
	
    public SemossMCPException(Throwable cause, MCPErrorCode error) {
        super(cause);
		this.error = error;
    }
	
	public SemossMCPException(String message, boolean continueThreadOfExecution, MCPErrorCode error) {
		super(message);
		this.error = error;
	}
	
	public SemossMCPException(NounMetadata noun, MCPErrorCode error) {
		super(noun);
		this.error = error;
	}
	
	/**
	 * 
	 * @return
	 */
	public MCPErrorCode getError() {
		return this.error;
	}
	
	/**
	 * Always kill the thread of execution
	 */
	@Override
	public boolean isContinueThreadOfExecution() {
		return false;
	}
}
