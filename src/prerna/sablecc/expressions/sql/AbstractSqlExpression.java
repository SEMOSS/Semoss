package prerna.sablecc.expressions.sql;

import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import prerna.ds.h2.H2Frame;
import prerna.sablecc.AbstractReactor;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.expressions.sql.builder.SqlColumnSelector;
import prerna.sablecc.expressions.sql.builder.SqlExpressionBuilder;
import prerna.sablecc.meta.IPkqlMetadata;
import prerna.sablecc.meta.MathPkqlMetadata;

public abstract class AbstractSqlExpression extends AbstractReactor {

	// this contains the sql builder to modify
	protected SqlExpressionBuilder builder;
	
	public AbstractSqlExpression() {
		String[] thisReacts = { PKQLEnum.EXPR_TERM, 
				PKQLEnum.DECIMAL, 
				PKQLEnum.NUMBER, 
				PKQLEnum.GROUP_BY, 
				PKQLEnum.COL_DEF, 
				PKQLEnum.MATH_PARAM};
		
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.MATH_FUN;
	}
	
	@Override
	public Iterator process() {
		H2Frame h2Frame = (H2Frame) myStore.get("G");

		// if this is wrapping an existing expression iterator
		if(myStore.get("TERM") instanceof SqlExpressionBuilder) {
			this.builder = (SqlExpressionBuilder) myStore.get("TERM");
		} else {
			// the input has to be a set of column as the starting point
			Vector<String> columns = (Vector<String>) myStore.get(PKQLEnum.COL_DEF);
			
			this.builder = new SqlExpressionBuilder(h2Frame);
			for(String col : columns) {
				SqlColumnSelector selector = new SqlColumnSelector(h2Frame, col);
				this.builder.addSelector(selector);
			}
		}
		
		return null;
	}
	
	public IPkqlMetadata getPkqlMetadata() {
		MathPkqlMetadata metadata = new MathPkqlMetadata();
		metadata.setProcedureName((String) myStore.get("PROC_NAME"));
		metadata.setPkqlStr((String) myStore.get("MATH_EXPRESSION"));
		metadata.setColumnsOperatedOn((Vector<String>) myStore.get(PKQLEnum.COL_DEF));
		metadata.setGroupByColumns((List<String>) myStore.get(PKQLEnum.COL_CSV)); 
		return metadata;
	}
	
}
