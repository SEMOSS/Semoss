package prerna.sablecc.expressions.sql.builder;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import prerna.ds.r.RDataTable;
import prerna.ds.rdbms.h2.H2Frame;
import prerna.sablecc.expressions.IExpressionBuilder;
import prerna.sablecc.expressions.IExpressionSelector;
import prerna.sablecc.expressions.r.builder.RArithmeticSelector;
import prerna.sablecc.expressions.r.builder.RColumnSelector;
import prerna.sablecc.expressions.r.builder.RConstantSelector;
import prerna.sablecc.expressions.r.builder.RExpressionBuilder;
import prerna.util.ArrayUtilityMethods;

public class ExpressionGenerator {

	public static SqlExpressionBuilder sqlGenerateSimpleMathExpressions(H2Frame frame, Object leftObj, Object rightObj, String mathSymbol) {
		String[] headers = frame.getColumnHeaders();
		SqlExpressionBuilder builder = new SqlExpressionBuilder(frame);
		
		// we have 3 cases for each object
		// it is a sql builder
		// it is a header
		// it is a constant
		
		IExpressionSelector leftSelector = null;
		List<IExpressionSelector> leftGroups = null;
		if(leftObj instanceof IExpressionBuilder) {
			// get the builder
			// get the last selector
			// get the group bys if present
			IExpressionBuilder leftBuilder = (IExpressionBuilder) leftObj;
			leftSelector = leftBuilder.getLastSelector();
			leftGroups = leftBuilder.getGroupBySelectors();
		} else if(ArrayUtilityMethods.arrayContainsValue(headers, leftObj.toString())) {
			// selector is a header
			leftSelector = new SqlColumnSelector(frame, leftObj.toString());
		} else {
			// selector is a constant
			leftSelector = new SqlConstantSelector(leftObj);
		}
		
		IExpressionSelector rightSelector = null;
		List<IExpressionSelector> rightGroups = null;
		if(rightObj instanceof IExpressionBuilder) {
			// get the builder
			// get the last selector
			// get the group bys if present
			IExpressionBuilder rightBuilder = (IExpressionBuilder) rightObj;
			rightSelector = rightBuilder.getLastSelector();
			rightGroups = rightBuilder.getGroupBySelectors();
		} else if(ArrayUtilityMethods.arrayContainsValue(headers, rightObj.toString())) {
			// selector is a header
			rightSelector = new SqlColumnSelector(frame, rightObj.toString());
		} else {
			// selector is a constant
			rightSelector = new SqlConstantSelector(rightObj);
		}

		// just doing a check to make sure the groups are the same
		// only need to do this if both are not null or empty
		if(leftGroups != null && leftGroups.size() > 0 && rightGroups != null && rightGroups.size() > 0) {
			
			Set<String> groupCols = new HashSet<String>();
			for(int i = 0; i < leftGroups.size(); i++) {
				groupCols.addAll(leftGroups.get(i).getTableColumns());
			}
			int startSize = groupCols.size();
			
			for(int i = 0; i < rightGroups.size(); i++) {
				groupCols.addAll(rightGroups.get(i).getTableColumns());
			}
			int endSize = groupCols.size();
			
			if(startSize != endSize) {
				throw new IllegalArgumentException("Expression contains group bys that are not the same.  Unable to process.");
			}
			
			// groups are good
			// doesn't matter which one we add
			for(int i = 0; i < rightGroups.size(); i++) {
				builder.addGroupBy(rightGroups.get(i));
			}
			
		} else {
			// one is empty
			// so just add whichever groups are present
			if(leftGroups != null && leftGroups.size() > 0) {
				for(int i = 0; i < leftGroups.size(); i++) {
					builder.addGroupBy(leftGroups.get(i));
				}
			} else if(rightGroups != null && rightGroups.size() > 0) {
				for(int i = 0; i < rightGroups.size(); i++) {
					builder.addGroupBy(rightGroups.get(i));
				}
			}
		}
		
		// now create the combined expression
		SqlArithmeticSelector newSelector = new SqlArithmeticSelector(leftSelector, rightSelector, mathSymbol);
		builder.addSelector(newSelector);
		
		return builder;
	}
	
	public static RExpressionBuilder rGenerateSimpleMathExpressions(RDataTable frame, Object leftObj, Object rightObj, String mathSymbol) {
		String[] headers = frame.getColumnHeaders();
		RExpressionBuilder builder = new RExpressionBuilder(frame);
		
		// we have 3 cases for each object
		// it is a sql builder
		// it is a header
		// it is a constant
		
		IExpressionSelector leftSelector = null;
		List<IExpressionSelector> leftGroups = null;
		if(leftObj instanceof IExpressionBuilder) {
			// get the builder
			// get the last selector
			// get the group bys if present
			IExpressionBuilder leftBuilder = (IExpressionBuilder) leftObj;
			leftSelector = leftBuilder.getLastSelector();
			leftGroups = leftBuilder.getGroupBySelectors();
		} else if(ArrayUtilityMethods.arrayContainsValue(headers, leftObj.toString())) {
			// selector is a header
			leftSelector = new RColumnSelector(frame, leftObj.toString());
		} else {
			// selector is a constant
			leftSelector = new RConstantSelector(leftObj);
		}
		
		IExpressionSelector rightSelector = null;
		List<IExpressionSelector> rightGroups = null;
		if(rightObj instanceof IExpressionBuilder) {
			// get the builder
			// get the last selector
			// get the group bys if present
			IExpressionBuilder rightBuilder = (IExpressionBuilder) rightObj;
			rightSelector = rightBuilder.getLastSelector();
			rightGroups = rightBuilder.getGroupBySelectors();
		} else if(ArrayUtilityMethods.arrayContainsValue(headers, rightObj.toString())) {
			// selector is a header
			rightSelector = new RColumnSelector(frame, rightObj.toString());
		} else {
			// selector is a constant
			rightSelector = new RConstantSelector(rightObj);
		}

		// just doing a check to make sure the groups are the same
		// only need to do this if both are not null or empty
		if(leftGroups != null && leftGroups.size() > 0 && rightGroups != null && rightGroups.size() > 0) {
			
			Set<String> groupCols = new HashSet<String>();
			for(int i = 0; i < leftGroups.size(); i++) {
				groupCols.addAll(leftGroups.get(i).getTableColumns());
			}
			int startSize = groupCols.size();
			
			for(int i = 0; i < rightGroups.size(); i++) {
				groupCols.addAll(rightGroups.get(i).getTableColumns());
			}
			int endSize = groupCols.size();
			
			if(startSize != endSize) {
				throw new IllegalArgumentException("Expression contains group bys that are not the same.  Unable to process.");
			}
			
			// groups are good
			// doesn't matter which one we add
			for(int i = 0; i < rightGroups.size(); i++) {
				builder.addGroupBy(rightGroups.get(i));
			}
			
		} else {
			// one is empty
			// so just add whichever groups are present
			if(leftGroups != null && leftGroups.size() > 0) {
				for(int i = 0; i < leftGroups.size(); i++) {
					builder.addGroupBy(leftGroups.get(i));
				}
			} else if(rightGroups != null && rightGroups.size() > 0) {
				for(int i = 0; i < rightGroups.size(); i++) {
					builder.addGroupBy(rightGroups.get(i));
				}
			}
		}
		
		// now create the combined expression
		RArithmeticSelector newSelector = new RArithmeticSelector(leftSelector, rightSelector, mathSymbol);
		builder.addSelector(newSelector);
		
		return builder;
	}
}
