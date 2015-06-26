package prerna.nameserver;

import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.engine.api.IEngine;

public interface INameServer {

	/**
	 * Add a question (insight) to the name server
	 * @param questionURL					The unique URL for the question
	 * @param paramURLs						URLs for the parameters
	 * @param tags							List of tags for the question
	 * @param filterOptions					Filtering options for the question
	 * @return 								boolean if adding the question was a success
	 */
	boolean indexQuestion(String questionURL, List<String> paramURLs, List<String> tags, Map<String, Object> filterOptions);
	
	/**
	 * Removes a question (insight) from the name server
	 * @param questionURL					The unique URL for the question
	 * @return								boolean if removing the question was a success
	 */
	boolean unIndexQuestion(String questionURL);
	
	/**
	 * Indexes an engine and all of its concepts/relationships into the name server
	 * @param engineName					The name of the engine
	 * @param owlEngine						The engine containing the metamodel for the name server
	 * @return
	 */
	boolean indexEngine(String engineName, IEngine owlEngine);
	
	/**
	 * Removes an engine and its concepts/relationships from the name server
	 * @param engineName					The name of the engine
	 * @return
	 */
	boolean unIndexEngine(String engineName);
	
	/**
	 * Get all the tags associated with a question
	 * @param questionURL					The unique URL for the question
	 * @return								The list of tags for the question
	 */
	List<INameServerTag> getTags(String questionURL);
	
	/**
	 * Add tags to an existing question
	 * @param questionURL					The unique URL for the question
	 * @return 								boolean if adding the tags was successful
	 */
	boolean addTags(String questionURL, List<INameServerTag> tags);
	
	/**
	 * Remove tags from an existing question
	 * @param questionURL					The unique URL for the question
	 * @return								boolean if deleting the tags was successful
	 */
	boolean deleteTags(String questionURL, List<INameServerTag> tags);
	
	/**
	 * Return a list of question URLs related to the tags
	 * @param tags							The list of tags to perform the search
	 * @return								The list of question URLs based on the search
	 */
	List<String> searchTags(List<INameServerTag> tags);
	
	/**
	 * Return a ConnectedConcepts object
	 * @param concept						The concept to determine what other concepts can be traversed from it
	 * @return								An object containing all the relevant information for the connection  
	 */
	ConnectedConcepts searchConnectedConcepts(String concept);
	
	/**
	 * Return a map between engine to a map of upstream/downstream to the list of concepts directly connected to the input concept 
	 * @param concept						The concept to determine what other concepts can be traversed from it
	 * @return								A map from engine to the strings "upstream" or "downstream" (to determine directionality of relationship) to a list of concepts as strings  
	 */
	Map<String, Map<String, Set<String>>> searchDirectlyConnectedConcepts(String concept);
	
	/**
	 * Return a list of question URLs related to the instances list
	 * @param instanceURIs					A list of instances to find related questions
	 * @return								The list of question URLs for the list of instances
	 */
	List<String> searchData(List<String> instanceURIs);
	
	/**
	 * Return a list of question URLs related to the search string
	 * @param searchString					A search string used to find related insights
	 * @return								A list of question URLs related to the search string
	 */
	List<String> searchQuestion(String searchString);
}
