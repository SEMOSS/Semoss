package prerna.sablecc.expressions.sql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import prerna.ds.rdbms.h2.H2Frame;
import prerna.sablecc.AbstractReactor;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLRunner.STATUS;
import prerna.sablecc.expressions.IExpressionSelector;
import prerna.sablecc.expressions.sql.builder.SqlColumnSelector;
import prerna.sablecc.expressions.sql.builder.SqlConstantSelector;
import prerna.sablecc.expressions.sql.builder.SqlExpressionBuilder;
import prerna.sablecc.meta.IPkqlMetadata;
import prerna.sablecc.meta.MathPkqlMetadata;

public abstract class AbstractH2SqlBaseReducer extends AbstractReactor {

	protected String routine = null;
	protected String pkqlRoutine = null;
	protected SqlExpressionBuilder builder = null;
	
	public AbstractH2SqlBaseReducer() {
		String[] thisReacts = { PKQLEnum.EXPR_TERM, PKQLEnum.DECIMAL, PKQLEnum.NUMBER, PKQLEnum.GROUP_BY, PKQLEnum.COL_DEF, PKQLEnum.MAP_OBJ};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.MATH_FUN;
	}
	
	public abstract SqlExpressionBuilder process(H2Frame frame, SqlExpressionBuilder builder);

	@Override
	public Iterator process() {
		String nodeStr = myStore.get(whoAmI).toString();
		H2Frame h2Frame = (H2Frame)myStore.get("G");

		boolean hasGroups = false;
		
		// if this is wrapping an existing expression iterator
		if(myStore.get("TERM") instanceof SqlExpressionBuilder) {
			this.builder = (SqlExpressionBuilder) myStore.get("TERM");
			
			// make sure groups are not contradicting
			// get the current groups
			Set<String> groups = new HashSet<String>();
			List<String> existingGroups = builder.getGroupByColumns();
			if(existingGroups != null && existingGroups.size() > 0) {
				hasGroups = true;
				groups.addAll(existingGroups);
				int startSize = groups.size();
				List<String> groupBys = (List <String>) myStore.get(PKQLEnum.COL_CSV);
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
			this.builder = new SqlExpressionBuilder(h2Frame);
			// this case can only be if we pass in a column
			
			// only other possibility we account for is a column
			// the input has to be a set of column as the starting point
			List<String> columns = (List<String>) myStore.get(PKQLEnum.COL_DEF);
			
			for(String col : columns) {
				SqlColumnSelector selector = new SqlColumnSelector(h2Frame, col);
				this.builder.addSelector(selector);
			}
			
			// no existing group bys
			// see if we need to add any new ones
			hasGroups = addGroupBys(h2Frame, this.builder);
		}

		this.builder = process(h2Frame, this.builder);

		// if no groups
		// consolidate into a single value
		if(!hasGroups){
			ResultSet rs = h2Frame.execQuery(this.builder.toString());
			Object result = null;
			try {
				rs.next();
				result = rs.getObject(1);
				// we just added a selector which we can reduce into a scalar
				IExpressionSelector lastSelector = this.builder.getLastSelector();
				SqlConstantSelector constant = new SqlConstantSelector(result);
				constant.setTableColumnsUsed(lastSelector.getTableColumns());
				this.builder.replaceSelector(lastSelector, constant);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		myStore.put(nodeStr, this.builder);
		myStore.put("STATUS",STATUS.SUCCESS);
		
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
		List<String> groupBys = (List <String>) myStore.get(PKQLEnum.COL_CSV);
		if(groupBys != null) {
			for(String groupBy : groupBys) {
				SqlColumnSelector gSelector = new SqlColumnSelector(h2Frame, groupBy);
				builder.addGroupBy(gSelector);
				hasGroups = true;
			}
		}
		return hasGroups;
	}
	
	public void setRoutine(String routine) {
		this.routine = routine;
	}
	
	public void setPkqlRoutine(String pkqlRoutine) {
		this.pkqlRoutine  = pkqlRoutine;
	}
	
	public IPkqlMetadata getPkqlMetadata() {
		MathPkqlMetadata metadata = new MathPkqlMetadata();
		metadata.setPkqlStr((String) myStore.get(PKQLEnum.MATH_FUN));
		metadata.setProcedureName(pkqlRoutine);
		metadata.setColumnsOperatedOn(this.builder.getAllTableColumnsUsed());
		metadata.setGroupByColumns(this.builder.getGroupByColumns()); 
		return metadata;
	}
}
