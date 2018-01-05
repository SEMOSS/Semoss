package prerna.query.querystruct.selectors;

import java.util.List;

public interface IQuerySelector {

	String PRIM_KEY_PLACEHOLDER = "PRIM_KEY_PLACEHOLDER";

	public enum QUERY_TYPE {BASE_SQL, SPARQL, R}
	public enum SELECTOR_TYPE {COLUMN, MATH, ARITHMETIC, CONSTANT}
	
	/**
	 * Determine the type of the selector
	 * @return
	 */
	SELECTOR_TYPE getSelectorType();
	
	/**
	 * Get the display name for the selector
	 * @return
	 */
	String getAlias();
	
	/**
	 * Set the display name for the selector
	 * @param alias
	 */
	void setAlias(String alias);
	
	/**
	 * Determine if it is a derived selector
	 * @return
	 */
	boolean isDerived();
	
	/**
	 * Return the predicted data type of the column
	 * @return
	 */
	String getDataType();
	
	/**
	 * Get the pixel component that generated the selector
	 * @return
	 */
	String getQueryStructName();
	
	/**
	 * Determine all the columns used within an expression
	 * @return
	 */
	List<QueryColumnSelector> getAllQueryColumns();
	
	// THIS IS SUPER DIFFICULT TO DO
	// REMEMBER, FE PASSES CONCEPTUAL NAMES WHICH NEED
	// TO BE TRANSLATED TO PHYSICAL NAMES
	// SO THIS WILL GET VERY COMPLICATED VERY QUICKLY
	// PUSHING RESPONSIBILITY TO THE INTERPRETERS...
//	/**
//	 * This will override the existing toString method to get
//	 * the appropriate expression string for the selector to execute
//	 * @return
//	 */
//	String getQuerySyntax(QUERY_TYPE type);

}
