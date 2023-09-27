package prerna.engine.api;

import java.util.List;

public interface IVectorDatabaseEngine extends IEngine {

	/**
	 * Gets the type of the model inference engine.  The model engine type is often used to determine what client to use while running questions
	 * @return the type of the database 
	*/
	VectorDatabaseTypeEnum getVectorDatabaseType();
	
	// i vector db
	// open close nearest neighbor, add document - remove document. question
	
	void addDocumet(List<String> fileNames) throws Exception;
	
	void removeDocument(List<String> fileNames) throws Exception;
	
	Object nearestNeighbor(String question, String limit);
}
