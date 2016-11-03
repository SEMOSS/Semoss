package prerna.sablecc.expressions.r;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.rosuda.REngine.REXP;
import org.rosuda.REngine.REXPMismatchException;

import prerna.ds.r.RDataTable;
import prerna.sablecc.AbstractReactor;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLRunner.STATUS;
import prerna.sablecc.expressions.r.builder.RColumnSelector;
import prerna.sablecc.expressions.r.builder.RConstantSelector;
import prerna.sablecc.expressions.r.builder.RExpressionBuilder;
import prerna.sablecc.expressions.sql.builder.SqlConstantSelector;

public abstract class AbstractRBaseReducer extends AbstractReactor {

	//TODO:
	//TODO:
	//TODO: need to add in the filters on the RFrame
	
	protected String mathRoutine = null;
	protected String pkqlMathRoutine = null;

	public AbstractRBaseReducer() {
		String[] thisReacts = { PKQLEnum.EXPR_TERM, PKQLEnum.DECIMAL, PKQLEnum.NUMBER, PKQLEnum.GROUP_BY, PKQLEnum.COL_DEF, PKQLEnum.MATH_PARAM};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.MATH_FUN;
	}
	
	public abstract RExpressionBuilder process(RDataTable frame, RExpressionBuilder builder);

	@Override
	public Iterator process() {
		String nodeStr = myStore.get(whoAmI).toString();
		RDataTable rDataTable = (RDataTable) myStore.get("G");

		boolean hasGroups = false;
		
		RExpressionBuilder builder = null;
		// if this is wrapping an existing expression iterator
		if(myStore.get("TERM") instanceof RExpressionBuilder) {
			builder = (RExpressionBuilder) myStore.get("TERM");
			
			// make sure groups are not contradicting
			// get the current groups
			Set<String> groups = new HashSet<String>();
			List<String> existingGroups = builder.getGroupByColumns();
			if(existingGroups != null && existingGroups.size() > 0) {
				hasGroups = true;
				groups.addAll(existingGroups);
				int startSize = groups.size();
				Vector<String> groupBys = (Vector <String>) myStore.get(PKQLEnum.COL_CSV);
				// when i remove these new groups, the size better be 0
				// meaning they are all already accounted for
				if(groupBys != null && !groupBys.isEmpty()) {
					groups.addAll(groupBys);
					int endSize = groups.size();
					if(startSize != endSize) {
						throw new IllegalArgumentException("Expression contains group bys that are not the same.  Unable to process.");
					}
				}
			} 
			else {
				// no existing group bys
				// see if we need to add any new ones
				hasGroups = addGroupBys(builder);
			}
		} else {
			builder = new RExpressionBuilder(rDataTable);
			// this case can only be if we pass in a column
			
			// modify the expression to get the sql syntax
			modExpression();
			// we have a new expression
			// input is a new column
			String column = myStore.get("MOD_" + whoAmI).toString();
			column = column.replace("[", "").replace("]", "").trim();
			
			RColumnSelector cSelector = new RColumnSelector(column);
			builder.addSelector(cSelector);
			
			// no existing group bys
			// see if we need to add any new ones
			hasGroups = addGroupBys(builder);
		}

		builder = process(rDataTable, builder);

		if(hasGroups){
			myStore.put(nodeStr, builder);
			myStore.put("STATUS",STATUS.SUCCESS);
		} else {
			REXP rs = rDataTable.executeRScript(builder.toString());
			Map<String, Object> result = null;
			try {
				result = (Map<String, Object>) rs.asNativeJavaObject();
				// we assume there is only 1 value to return
				Object value = result.get(result.keySet().iterator().next());
				Object val = null;
				if(value instanceof Object[]) {
					val = ((Object[]) value)[0];
				} else if(value instanceof double[]) {
					val = ((double[]) value)[0];
				} else if( value instanceof int[]) {
					val = ((int[]) value)[0];
				} else {
					val = value;
				}
				// we just added a selector which we can reduce into a scalar
				RConstantSelector constant = new RConstantSelector(val);
				builder.replaceSelector(builder.getLastSelector(), constant);
			} catch (REXPMismatchException e) {
				e.printStackTrace();
			}
			
			myStore.put(nodeStr, builder);
			myStore.put("STATUS",STATUS.SUCCESS);
		}
		
		return null;
	}
	
	/**
	 * Adds the group bys and returns if groups bys were added
	 * @param h2Frame
	 * @param builder
	 * @return
	 */
	private boolean addGroupBys(RExpressionBuilder builder) {
		boolean hasGroups = false;
		Vector<String> groupBys = (Vector <String>) myStore.get(PKQLEnum.COL_CSV);
		if(groupBys != null) {
			for(String groupBy : groupBys) {
				RColumnSelector gSelector = new RColumnSelector(groupBy);
				builder.addGroupBy(gSelector);
				hasGroups = true;
			}
		}
		return hasGroups;
	}
	
	public void setMathRoutine(String mathRoutine) {
		this.mathRoutine = mathRoutine;
	}
	
	public void setPkqlMathRoutine(String pkqlMathRoutine) {
		this.pkqlMathRoutine  = pkqlMathRoutine;
	}
	
}
