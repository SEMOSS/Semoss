package prerna.engine.impl.model.responses;

/** 
 * The {@code IModelEngineResponseStreamHandler} interface should be implemented for any {@code IModelEngine} that 
 * makes inference calls using REST directly and allows for streaming. It defines how partial responses from a REST call should be
 * retrieved.
 * 
 * <p>
 * This interface is typically used in conjunction with {@code IModelEngineResponseHandler} implementations.
*/
public interface IModelEngineResponseStreamHandler {
		
	Object getPartialResponse();
}
