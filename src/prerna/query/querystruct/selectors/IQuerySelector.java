package prerna.query.querystruct.selectors;

import java.util.List;

import prerna.util.gson.AbstractSemossTypeAdapter;
import prerna.util.gson.QueryArithmeticSelectorAdapter;
import prerna.util.gson.QueryColumnSelectorAdapter;
import prerna.util.gson.QueryConstantSelectorAdapter;
import prerna.util.gson.QueryFunctionSelectorAdapter;
import prerna.util.gson.QueryIfSelectorAdapter;
import prerna.util.gson.QueryOpaqueSelectorAdapter;

public interface IQuerySelector {

	String PRIM_KEY_PLACEHOLDER = "PRIM_KEY_PLACEHOLDER";

	enum SELECTOR_TYPE {OPAQUE, COLUMN, FUNCTION, ARITHMETIC, CONSTANT, IF_ELSE}
	
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

	////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////

	/*
	 * 
	 * Methods around serialization
	 * 
	 */
	
	static AbstractSemossTypeAdapter getAdapterForSelector(SELECTOR_TYPE type) {
		if(type == SELECTOR_TYPE.OPAQUE) {
			return new QueryOpaqueSelectorAdapter();
		} else if(type == SELECTOR_TYPE.COLUMN) {
			return new QueryColumnSelectorAdapter();
		} else if(type == SELECTOR_TYPE.FUNCTION) {
			return new QueryFunctionSelectorAdapter();
		} else if(type == SELECTOR_TYPE.ARITHMETIC) {
			return new QueryArithmeticSelectorAdapter();
		} else if(type == SELECTOR_TYPE.CONSTANT) {
			return new QueryConstantSelectorAdapter();
		} else if(type == SELECTOR_TYPE.IF_ELSE) {
			return new QueryIfSelectorAdapter();
		}
		
		return null;
	}
	
	/**
	 * Convert string to SELECTOR_TYPE
	 * @param s
	 * @return
	 */
	static SELECTOR_TYPE convertStringToSelectorType(String s) {
		if(s.equals(SELECTOR_TYPE.OPAQUE.toString())) {
			return SELECTOR_TYPE.OPAQUE;
		} else if(s.equals(SELECTOR_TYPE.COLUMN.toString())) {
			return SELECTOR_TYPE.COLUMN;
		} else if(s.equals(SELECTOR_TYPE.FUNCTION.toString())) {
			return SELECTOR_TYPE.FUNCTION;
		} else if(s.equals(SELECTOR_TYPE.ARITHMETIC.toString())) {
			return SELECTOR_TYPE.ARITHMETIC;
		} else if(s.equals(SELECTOR_TYPE.CONSTANT.toString())) {
			return SELECTOR_TYPE.CONSTANT;
		} else if(s.equals(SELECTOR_TYPE.IF_ELSE.toString())) {
			return SELECTOR_TYPE.IF_ELSE;
		}
		return null;
	}
	
	/**
	 * Get the class for each selector type
	 * @param type
	 * @return
	 */
	static Class getQuerySelectorClassFromType(SELECTOR_TYPE type) {
		if(type == SELECTOR_TYPE.OPAQUE) {
			return QueryOpaqueSelector.class;
		} else if(type == SELECTOR_TYPE.COLUMN) {
			return QueryColumnSelector.class;
		} else if(type == SELECTOR_TYPE.FUNCTION) {
			return QueryFunctionSelector.class;
		} else if(type == SELECTOR_TYPE.ARITHMETIC) {
			return QueryArithmeticSelector.class;
		} else if(type == SELECTOR_TYPE.CONSTANT) {
			return QueryConstantSelector.class;
		} else if(type == SELECTOR_TYPE.IF_ELSE) {
			return QueryIfSelector.class;
		}
		
		return null;
	}
	
}

