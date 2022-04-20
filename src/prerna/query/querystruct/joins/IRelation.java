package prerna.query.querystruct.joins;

import com.google.gson.TypeAdapter;

import prerna.util.gson.BasicRelationshipAdapter;
import prerna.util.gson.SubqueryRelationshipAdapter;

public interface IRelation {

	enum RELATION_TYPE {BASIC, SUBQUERY}
	
	RELATION_TYPE getRelationType();
	
	////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////

	/*
	 * 
	 * Methods around serialization
	 * 
	 */

	static TypeAdapter getAdapterForRelation(RELATION_TYPE type) {
		if(type == RELATION_TYPE.BASIC) {
			return new BasicRelationshipAdapter();
		} else if(type == RELATION_TYPE.SUBQUERY) {
			return new SubqueryRelationshipAdapter();
		}
		
		return null;
	}
	
	/**
	 * Convert string to SELECTOR_TYPE
	 * @param s
	 * @return
	 */
	static RELATION_TYPE convertStringToRelationType(String s) {
		if(s.equals(RELATION_TYPE.BASIC.toString())) {
			return RELATION_TYPE.BASIC;
		} else if(s.equals(RELATION_TYPE.SUBQUERY.toString())) {
			return RELATION_TYPE.SUBQUERY;
		} 
		
		return null;
	}
}
