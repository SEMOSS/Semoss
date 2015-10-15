package prerna.rdf.query.util;

public interface ISPARQLFilterInput {

	// different types of filter options
	enum FILTER_TYPE {REGEX_MATCH, NUMERIC_CONSTRAINT, URI_MATCH}
	
	// regardless of filter type, we just need to get a string that states a single condition from each filter input
	String getFilterInput();

	public Object getVar();
	
	public Object getValue();
}
