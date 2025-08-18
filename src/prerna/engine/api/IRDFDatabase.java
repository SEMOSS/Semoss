package prerna.engine.api;

import java.util.List;

public interface IRDFDatabase extends IDatabaseEngine {

	static boolean isRDFDbType(IDatabaseEngine.DATABASE_TYPE type) {
		if(type == IDatabaseEngine.DATABASE_TYPE.SESAME
				|| type == IDatabaseEngine.DATABASE_TYPE.JENA 
				|| type == IDatabaseEngine.DATABASE_TYPE.RDF4J 
				|| type == IDatabaseEngine.DATABASE_TYPE.JENA_TDB) {
			return true;
		}
		return false;
	}
	
	/**
	 * Processes a given subject, predicate, object triple and adds the statement to the SailConnection.
	 * @param args array contains the following
	 * 				subject String - RDF Subject
	 * 				predicate String - RDF Predicate
	 * 				object Object - RDF Object
	 * 				concept boolean - True if the statement is a concept (URI), False if it is a property (Literal)
	 */
	void addStatement(Object[] args);
	
	/**
	 * Processes a given subject, predicate, object triple and removes the statement to the SailConnection.
	 * @param args array contains the following
	 * 				subject String - RDF Subject
	 * 				predicate String - RDF Predicate
	 * 				object Object - RDF Object
	 * 				concept boolean - True if the statement is a concept (URI), False if it is a property (Literal)
	 */
	void removeStatement(Object[] args);
	
	/**
	 * Processes a bulk insertion of triples to the SailConnection.
	 * @param args List of array contains the following
	 * 				subject String - RDF Subject
	 * 				predicate String - RDF Predicate
	 * 				object Object - RDF Object
	 * 				concept boolean - True if the statement is a concept (URI), False if it is a property (Literal)
	 * @param commit 
	 */
	void bulkInsert(List<Object[]> args);
	
	/**
	 * Processes a bulk removal of triples to the SailConnection.
	 * @param args List of array contains the following
	 * 				subject String - RDF Subject
	 * 				predicate String - RDF Predicate
	 * 				object Object - RDF Object
	 * 				concept boolean - True if the statement is a concept (URI), False if it is a property (Literal)
	 * @param commit
	 */
	void bulkRemoval(List<Object[]> args);

	/**
	 * Persist inferencing in the model
	 * @throws Exception
	 */
	void infer() throws Exception;
	
	@Deprecated
	// TODO: replace and set this within the commit
	void exportDB() throws Exception;

}
