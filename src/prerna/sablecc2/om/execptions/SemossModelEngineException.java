package prerna.sablecc2.om.execptions;

import prerna.engine.impl.model.responses.AskModelEngineResponse;

public class SemossModelEngineException extends RuntimeException {
    
    private final AskModelEngineResponse<?> errorResponse;

    public SemossModelEngineException(AskModelEngineResponse<?> errorResponse) {
        super(errorResponse.getStringResponse());
        this.errorResponse = errorResponse;
    }

    public AskModelEngineResponse<?> getErrorResponse() {
        return errorResponse;
    }
}