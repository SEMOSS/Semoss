package prerna.query.querystruct.selectors;

import java.util.List;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.query.querystruct.selectors.adapters.QueryArithmeticSelectorAdapter;
import prerna.query.querystruct.selectors.adapters.QueryColumnSelectorAdapter;
import prerna.query.querystruct.selectors.adapters.QueryConstantSelectorAdapter;
import prerna.query.querystruct.selectors.adapters.QueryFunctionSelectorAdapter;
import prerna.query.querystruct.selectors.adapters.QueryOpaqueSelectorAdapter;

public interface IQuerySelector {

	String PRIM_KEY_PLACEHOLDER = "PRIM_KEY_PLACEHOLDER";

	enum SELECTOR_TYPE {OPAQUE, COLUMN, FUNCTION, ARITHMETIC, CONSTANT}
	
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

	static Gson getGson() {
		GsonBuilder gson = new GsonBuilder();
		gson.registerTypeAdapter(QueryColumnSelector.class, new QueryColumnSelectorAdapter());
		gson.registerTypeAdapter(QueryFunctionSelector.class, new QueryFunctionSelectorAdapter());
		gson.registerTypeAdapter(QueryArithmeticSelector.class, new QueryArithmeticSelectorAdapter());
		gson.registerTypeAdapter(QueryConstantSelector.class, new QueryConstantSelectorAdapter());
		gson.registerTypeAdapter(QueryOpaqueSelector.class, new QueryOpaqueSelectorAdapter());
		return gson.create();
	}
	
	static GsonBuilder appendQueryAdapters(GsonBuilder gson) {
		gson.registerTypeAdapter(QueryColumnSelector.class, new QueryColumnSelectorAdapter());
		gson.registerTypeAdapter(QueryFunctionSelector.class, new QueryFunctionSelectorAdapter());
		gson.registerTypeAdapter(QueryArithmeticSelector.class, new QueryArithmeticSelectorAdapter());
		gson.registerTypeAdapter(QueryConstantSelector.class, new QueryConstantSelectorAdapter());
		gson.registerTypeAdapter(QueryOpaqueSelector.class, new QueryOpaqueSelectorAdapter());
		return gson;
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
		}
		
		return null;
	}
	
}






