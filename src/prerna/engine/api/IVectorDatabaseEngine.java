package prerna.engine.api;

import java.util.List;
import java.util.Map;

public interface IVectorDatabaseEngine extends IEngine {

	
	// this is what the FE sends for the type of storage we are creating
	// as a result, cannot be a key in the smss file
	String VECTOR_TYPE = "VECTOR_TYPE";
		
	/**
	 * Gets the type of the model inference engine.  The model engine type is often used to determine what client to use while running questions
	 * @return the type of the database 
	*/
	VectorDatabaseTypeEnum getVectorDatabaseType();
	
	// i vector db
	// open close nearest neighbor, add document - remove document. question
	
	void addDocument(List<String> filePaths, Map <String, Object> parameters);
	
	void removeDocument(List<String> filePaths, Map <String, Object> parameters);
	
	Object nearestNeighbor(String question, int limit, Map <String, Object> parameters);
	
	List<Map<String, Object>> listDocuments(Map <String, Object> parameters);
}