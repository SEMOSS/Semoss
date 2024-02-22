package prerna.engine.impl.model.responses;

import java.util.List;
import java.util.Map;

/** 
 * The {@code IModelEngineResponseHandler} interface should be implemented for any {@code IModelEngine} that 
 * makes inference calls using REST directly. It defines how responses from a REST call should be
 * built or consolidated so that a standard model engine response can be created.
 * <p>
 * 
 * This interface defines the structure for handling responses from a model engine, 
 * which potentially involves streaming data.
*/
public interface IModelEngineResponseHandler {
	
	/**
	 * This method is intended to append or add a stream (or partial response) to the handler. 
	 * <p>
	 * It takes an instance of IModelEngineResponseStreamHandler as its parameter, 
	 * which represents a part of the overall response from the model engine.
	 * 
	 * @param partial
	*/
	void appendStream(IModelEngineResponseStreamHandler partial);
	
	/**
	 * This method is used to created the full response after it has been
	 * assimilated from partial responses
	*/
	void setResponse(Object response);
	
	/**
	 * This method returns a list of partial responses. 
	 * Each item in the list is an instance of {@code IModelEngineResponseStreamHandler}, 
	 * representing a segment or part of the full response from the model engine.
	*/
	List<IModelEngineResponseStreamHandler> getPartialResponses();
	
	/**
	 * This method returns the Class type of the stream handler. It ensures that the returned 
	 * class is a subclass of IModelEngineResponseStreamHandler. 
	 * <p>
	 * This is used to instantiate a {@code IModelEngineResponseStreamHandler} when processing the request response. 
	*/
	Class<? extends IModelEngineResponseStreamHandler> getStreamHandlerClass();
	
	/**
	 * This method returns a map which should contain the following keys:
	 * <li> response </li>
	 * <li> numberOfTokensInPrompt </li>
	 * <li> numberOfTokensInResponse </li>
	 * 
	 * <p>
	 * Ideally the values for these keys come from {@code getResponse}, {@code getPromptTokens}, 
	 * and {@code getResponseTokens} respectively.
	*/
	Map<String, Object> getModelEngineResponse();

	/**
	 * This method retrieves the complete response from the model engine. 
	 * The return type is generic (Object), since the response could be anything.
	*/
	Object getResponse();
	
	/**
	 * This method returns the number of tokens in the prompt that was sent to the {@code IModelEngine} class. 
	 * Tokens are typically units of text, like words or sentences, depending on the model's tokenization scheme.
	*/
	Integer getPromptTokens();
	
	/**
	 * This method returns the number of tokens in the text generation response from the {@code IModelEngine} class. 
	 * Tokens are typically units of text, like words or sentences, depending on the model's tokenization scheme.
	*/
	Integer getResponseTokens();	
}