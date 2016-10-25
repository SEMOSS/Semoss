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
//		String[] headers = frame.getColumnHeaders();
		// we override the toString method to return the script for the expression
		// so doing string concatenation should provide the desired result
		
		// need to account for if the frame is joined
		String leftExpr = leftObj.toString().trim();
		if( frame.getTableColumnName(leftExpr) != null) {
			leftExpr = frame.getTableColumnName(leftExpr);
		}
		String rightExpr = rightObj.toString().trim();
		if( frame.getTableColumnName(rightExpr) != null) {
			rightExpr = frame.getTableColumnName(rightExpr);
		}
		
		String combineExpression = leftExpr + " " + mathSymbol + " " + rightExpr;
		
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
		} 
//		else {
//			// this is the case it is a column def
//			if(ArrayUtilityMethods.arrayContainsValueIgnoreCase(headers, leftObj.toString())) {
//				joinCols.add(leftObj.toString());
//			}
//		}
		
		// repeat above for the rightObj
		if(rightObj instanceof H2SqlExpressionIterator) {
			// get the join columns
			String[] joins = ((H2SqlExpressionIterator) rightObj).getJoinColumns();
			if(joins != null) {
				for(int i = 0; i < joins.length; i++) {
					joinCols.add(joins[i]);
				}
			}
			
			// if we already have group by columns
			// then they need to be the same or this operation makes no sense
			int startGroupSize = groupCols.size();
			
			// get the group by columns
			String[] groups = ((H2SqlExpressionIterator) rightObj).getGroupColumns();
			if(groups != null) {
				for(int i = 0; i < groups.length; i++) {
					groupCols.add(groups[i]);
				}
			}

			int endGroupSize = groupCols.size();
			
			if(startGroupSize > 0 && startGroupSize != endGroupSize) {
				throw new IllegalArgumentException("Expression contains group bys that are not the same.  Unable to process.");
			}
			
			// should also close the rs
			((H2SqlExpressionIterator) rightObj).close();
		}
//		else {
//			// this is the case it is a column def
//			if(ArrayUtilityMethods.arrayContainsValueIgnoreCase(headers, rightObj.toString())) {
//				joinCols.add(rightObj.toString());
//			}
//		}
		
		String[] joins = joinCols.toArray(new String[]{});
		String[] groups = groupCols.toArray(new String[]{});
		String newCol = "newCol_" + Utility.getRandomString(6);
		
		H2SqlExpressionIterator it = new H2SqlExpressionIterator(frame, combineExpression, newCol, joins, groups);
		return it;
	}

	
	
	public static H2SqlExpressionIterator combineExpressionsForMultipleReturns(H2Frame frame, Object leftObj, Object rightObj) {
		String[] headers = frame.getColumnHeaders();
		// we override the toString method to return the script for the expression
		// so doing string concatenation should provide the desired result
		StringBuilder combineExpression = new StringBuilder(); 
		
		// will probably need the combination of the join columns for the new expression
		Set<String> joinCols = new HashSet<String>();
		// will probably need the combination of the group by columns for the new expression
		Set<String> groupCols = new HashSet<String>();
				
		if(leftObj instanceof H2SqlExpressionIterator) {
			H2SqlExpressionIterator it = (H2SqlExpressionIterator) leftObj;
			// get the join columns
			String[] joins = it.getJoinColumns();
			if(joins != null) {
				for(int i = 0; i < joins.length; i++) {
					joinCols.add(joins[i]);
				}
			}
			// get the group by columns
			String[] groups = it.getGroupColumns();
			if(groups != null) {
				for(int i = 0; i < groups.length; i++) {
					groupCols.add(groups[i]);
				}
			}

			if(it.getNewColumnName() != null) {
				combineExpression.append("( ").append(it.getExpression()).append(" ) AS ").append(it.getNewColumnName());
			}
			
			// should also close the rs
			it.close();
		} else {
			// this is the case it is a column def
			if(ArrayUtilityMethods.arrayContainsValueIgnoreCase(headers, leftObj.toString())) {
				joinCols.add(leftObj.toString());
			}
			
			combineExpression.append(leftObj.toString());
		}
		
		// separation between expressions
		
		if(combineExpression.length() > 0) {
			combineExpression.append(" , ");
		}
		
		// repeat above for the rightObj
		if(rightObj instanceof H2SqlExpressionIterator) {
			H2SqlExpressionIterator it = (H2SqlExpressionIterator) rightObj;

			// get the join columns
			String[] joins = it.getJoinColumns();
			if(joins != null) {
				for(int i = 0; i < joins.length; i++) {
					joinCols.add(joins[i]);
				}
			}
			
			// get the group by columns
			// if we already have group by columns
			// then they need to be the same or this operation makes no sense
			int startGroupSize = groupCols.size();
			
			// get the group by columns
			String[] groups = ((H2SqlExpressionIterator) rightObj).getGroupColumns();
			if(groups != null) {
				for(int i = 0; i < groups.length; i++) {
					groupCols.add(groups[i]);
				}
			}

			int endGroupSize = groupCols.size();
			
			if(startGroupSize > 0 && startGroupSize != endGroupSize) {
				throw new IllegalArgumentException("Expression contains group bys that are not the same.  Unable to process.");
			}
			
			if(it.getNewColumnName() != null) {
				combineExpression.append("( ").append(it.getExpression()).append(" ) AS ").append(it.getNewColumnName());
			}

			// should also close the rs
			it.close();
		} else {
			// this is the case it is a column def
			if(ArrayUtilityMethods.arrayContainsValueIgnoreCase(headers, rightObj.toString())) {
				joinCols.add(rightObj.toString());
			}
			
			combineExpression.append(rightObj.toString());
		}
		
		String[] joins = joinCols.toArray(new String[]{});
		String[] groups = groupCols.toArray(new String[]{});
		
		H2SqlExpressionIterator it = new H2SqlExpressionIterator(frame, combineExpression.toString(), null, joins, groups);
		return it;
	}

	
}
