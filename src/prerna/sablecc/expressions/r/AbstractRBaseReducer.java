package prerna.sablecc.expressions.r;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import prerna.ds.r.RDataTable;
import prerna.sablecc.AbstractReactor;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLRunner.STATUS;
import prerna.sablecc.expressions.IExpressionSelector;
import prerna.sablecc.expressions.r.builder.RColumnSelector;
import prerna.sablecc.expressions.r.builder.RConstantSelector;
import prerna.sablecc.expressions.r.builder.RExpressionBuilder;
import prerna.sablecc.meta.IPkqlMetadata;
import prerna.sablecc.meta.MathPkqlMetadata;

public abstract class AbstractRBaseReducer extends AbstractReactor {

	//TODO:
	//TODO:
	//TODO: need to add in the filters on the RFrame
	
	protected String mathRoutine = null;
	protected String pkqlMathRoutine = null;
	protected boolean castAsNumber = true;
	protected RExpressionBuilder builder = null;
	
	public AbstractRBaseReducer() {
		String[] thisReacts = { PKQLEnum.EXPR_TERM, PKQLEnum.DECIMAL, PKQLEnum.NUMBER, PKQLEnum.GROUP_BY, PKQLEnum.COL_DEF, PKQLEnum.MAP_OBJ};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.MATH_FUN;
	}
	
	public abstract RExpressionBuilder process(RDataTable frame, RExpressionBuilder builder);

	@Override
	public Iterator process() {
		String nodeStr = myStore.get(whoAmI).toString();
		RDataTable rDataTable = (RDataTable) myStore.get("G");

		boolean hasGroups = false;
		
		// if this is wrapping an existing expression iterator
		if(myStore.get("TERM") instanceof RExpressionBuilder) {
			this.builder = (RExpressionBuilder) myStore.get("TERM");
			
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
			this.builder = new RExpressionBuilder(rDataTable);
			// this case can only be if we pass in a column
			
			// only other possibility we account for is a column
			// the input has to be a set of column as the starting point
			Vector<String> columns = (Vector<String>) myStore.get(PKQLEnum.COL_DEF);
			
			for(String col : columns) {
				RColumnSelector cSelector = new RColumnSelector(rDataTable, col);
				this.builder.addSelector(cSelector);
			}
			
			// no existing group bys
			// see if we need to add any new ones
			hasGroups = addGroupBys(builder);
		}

		this.builder = process(rDataTable, builder);

		if(hasGroups){
			myStore.put(nodeStr, builder);
			myStore.put("STATUS",STATUS.SUCCESS);
		} else {
			// we assume there is only 1 value to return
			Object val = null; // rDataTable.getScalarValue(builder.toString());
			IExpressionSelector lastSelector = this.builder.getLastSelector();
			RConstantSelector constant = new RConstantSelector(val);
			constant.setTableColumnsUsed(lastSelector.getTableColumns());
			this.builder.replaceSelector(lastSelector, constant);
				
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
		RDataTable rDataTable = (RDataTable) myStore.get("G");

		boolean hasGroups = false;
		Vector<String> groupBys = (Vector <String>) myStore.get(PKQLEnum.COL_CSV);
		if(groupBys != null) {
			for(String groupBy : groupBys) {
				RColumnSelector gSelector = new RColumnSelector(rDataTable, groupBy);
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
	
	public void setCastAsNumber(boolean castAsNumber) {
		this.castAsNumber = castAsNumber;
	}
	
	public IPkqlMetadata getPkqlMetadata() {
		MathPkqlMetadata metadata = new MathPkqlMetadata();
		metadata.setPkqlStr((String) myStore.get(PKQLEnum.MATH_FUN));
		metadata.setProcedureName(pkqlMathRoutine);
		metadata.setColumnsOperatedOn(this.builder.getAllTableColumnsUsed());
		metadata.setGroupByColumns(this.builder.getGroupByColumns()); 
		return metadata;
	}
	
}
