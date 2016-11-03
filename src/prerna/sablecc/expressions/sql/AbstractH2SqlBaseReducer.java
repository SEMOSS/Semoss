package prerna.sablecc.expressions.sql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import prerna.ds.h2.H2Frame;
import prerna.sablecc.AbstractReactor;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLRunner.STATUS;
import prerna.sablecc.expressions.sql.builder.SqlColumnSelector;
import prerna.sablecc.expressions.sql.builder.SqlConstantSelector;
import prerna.sablecc.expressions.sql.builder.SqlExpressionBuilder;

public abstract class AbstractH2SqlBaseReducer extends AbstractReactor {

	protected String mathRoutine = null;
	protected String pkqlMathRoutine = null;

	public AbstractH2SqlBaseReducer() {
		String[] thisReacts = { PKQLEnum.EXPR_TERM, PKQLEnum.DECIMAL, PKQLEnum.NUMBER, PKQLEnum.GROUP_BY, PKQLEnum.COL_DEF, PKQLEnum.MATH_PARAM};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.MATH_FUN;
	}
	
	public abstract SqlExpressionBuilder process(H2Frame frame, SqlExpressionBuilder builder);

	@Override
	public Iterator process() {
		String nodeStr = myStore.get(whoAmI).toString();
		H2Frame h2Frame = (H2Frame)myStore.get("G");

		boolean hasGroups = false;
		
		SqlExpressionBuilder builder = null;
		// if this is wrapping an existing expression iterator
		if(myStore.get("TERM") instanceof SqlExpressionBuilder) {
			builder = (SqlExpressionBuilder) myStore.get("TERM");
			
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
				hasGroups = addGroupBys(h2Frame, builder);
			}
		} else {
			builder = new SqlExpressionBuilder(h2Frame);
			// this case can only be if we pass in a column
			
			// modify the expression to get the sql syntax
			modExpression();
			// we have a new expression
			// input is a new column
			String column = myStore.get("MOD_" + whoAmI).toString();
			column = column.replace("[", "").replace("]", "").trim();
			
			SqlColumnSelector cSelector = new SqlColumnSelector(h2Frame, column);
			builder.addSelector(cSelector);
			
			// no existing group bys
			// see if we need to add any new ones
			hasGroups = addGroupBys(h2Frame, builder);
		}

		builder = process(h2Frame, builder);

		if(hasGroups){
			myStore.put(nodeStr, builder);
			myStore.put("STATUS",STATUS.SUCCESS);
		} else {
			ResultSet rs = h2Frame.execQuery(builder.toString());
			Object result = null;
			try {
				rs.next();
				result = rs.getObject(1);
				// we just added a selector which we can reduce into a scalar
				SqlConstantSelector constant = new SqlConstantSelector(result);
				builder.replaceSelector(builder.getLastSelector(), constant);
			} catch (SQLException e) {
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
	private boolean addGroupBys(H2Frame h2Frame, SqlExpressionBuilder builder) {
		boolean hasGroups = false;
		Vector<String> groupBys = (Vector <String>) myStore.get(PKQLEnum.COL_CSV);
		if(groupBys != null) {
			for(String groupBy : groupBys) {
				SqlColumnSelector gSelector = new SqlColumnSelector(h2Frame, groupBy);
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
