package prerna.sablecc.expressions.sql;

import java.util.HashSet;
import java.util.Set;

import prerna.ds.H2.H2Frame;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Utility;

public class H2SqlExpressionGenerator {

	private H2SqlExpressionGenerator() {
		
	}
	
	public static H2SqlExpressionIterator generateSimpleMathExpressions(H2Frame frame, Object leftObj, Object rightObj, String mathSymbol) {
		String[] headers = frame.getColumnHeaders();
		// we override the toString method to return the script for the expression
		// so doing string concatenation should provide the desired result
		String combineExpression = leftObj.toString() + " " + mathSymbol + " " + rightObj;
		
		// will probably need the combination of the join columns for the new expression
		Set<String> joinCols = new HashSet<String>();
		// will probably need the combination of the group by columns for the new expression
		Set<String> groupCols = new HashSet<String>();
				
		if(leftObj instanceof H2SqlExpressionIterator) {
			// get the join columns
			String[] joins = ((H2SqlExpressionIterator) leftObj).getJoinColumns();
			if(joins != null) {
				for(int i = 0; i < joins.length; i++) {
					joinCols.add(joins[i]);
				}
			}
			// get the group by columns
			String[] groups = ((H2SqlExpressionIterator) leftObj).getGroupColumns();
			if(groups != null) {
				for(int i = 0; i < groups.length; i++) {
					groupCols.add(groups[i]);
				}
			}

			// should also close the rs
			((H2SqlExpressionIterator) leftObj).close();
		} else {
			// this is the case it is a column def
			if(ArrayUtilityMethods.arrayContainsValueIgnoreCase(headers, leftObj.toString())) {
				joinCols.add(leftObj.toString());
			}
		}
		
		// repeat above for the rightObj
		if(rightObj instanceof H2SqlExpressionIterator) {
			// get the join columns
			String[] joins = ((H2SqlExpressionIterator) rightObj).getJoinColumns();
			if(joins != null) {
				for(int i = 0; i < joins.length; i++) {
					joinCols.add(joins[i]);
				}
			}
			// get the group by columns
			String[] groups = ((H2SqlExpressionIterator) rightObj).getGroupColumns();
			if(groups != null) {
				for(int i = 0; i < groups.length; i++) {
					groupCols.add(groups[i]);
				}
			}

			// should also close the rs
			((H2SqlExpressionIterator) rightObj).close();
		} else {
			// this is the case it is a column def
			if(ArrayUtilityMethods.arrayContainsValueIgnoreCase(headers, rightObj.toString())) {
				joinCols.add(rightObj.toString());
			}
		}
		
		String[] joins = joinCols.toArray(new String[]{});
		String[] groups = groupCols.toArray(new String[]{});
		String newCol = "newCol_" + Utility.getRandomString(6);
		
		H2SqlExpressionIterator it = new H2SqlExpressionIterator(frame, combineExpression, newCol, joins, groups);
		return it;
	}

	
	
}
