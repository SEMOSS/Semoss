package prerna.engine.impl.model.responses;

public class AskStringModelEngineResponse extends AskModelEngineResponse<String> {

    private static final long serialVersionUID = 1L;

    public AskStringModelEngineResponse(String response, Integer numberOfTokensInPrompt, Integer numberOfTokensInResponse) {
        super(response, numberOfTokensInPrompt, numberOfTokensInResponse);
    }

	@Override
	public String getStringResponse() {
		// TODO Auto-generated method stub
		return this.getResponse();
	}

    // Additional methods specific to string responses can be added here
}