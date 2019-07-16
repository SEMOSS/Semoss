package prerna.sablecc.expressions.sql;

import java.util.Iterator;
import java.util.Vector;

import prerna.ds.rdbms.h2.H2Frame;
import prerna.sablecc.AbstractReactor;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.expressions.sql.builder.SqlColumnSelector;
import prerna.sablecc.expressions.sql.builder.SqlExpressionBuilder;
import prerna.sablecc.meta.IPkqlMetadata;
import prerna.sablecc.meta.MathPkqlMetadata;

public abstract class AbstractSqlExpression extends AbstractReactor {

	// this contains the sql builder to modify
	protected SqlExpressionBuilder builder;
	
	// store the name of the procedure
	protected String procedureName;
	
	public AbstractSqlExpression() {
		String[] thisReacts = { PKQLEnum.EXPR_TERM, 
				PKQLEnum.DECIMAL, 
				PKQLEnum.NUMBER, 
				PKQLEnum.GROUP_BY, 
				PKQLEnum.COL_DEF, 
				PKQLEnum.MAP_OBJ};
		
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
	
	protected void setProcedureName(String procedureName) {
		this.procedureName = procedureName;
	}
	
	public IPkqlMetadata getPkqlMetadata() {
		MathPkqlMetadata metadata = new MathPkqlMetadata();
		metadata.setPkqlStr((String) myStore.get(PKQLEnum.MATH_FUN));
		metadata.setProcedureName(procedureName);
		metadata.setColumnsOperatedOn(this.builder.getAllTableColumnsUsed());
		metadata.setGroupByColumns(this.builder.getGroupByColumns()); 
		return metadata;
	}
	
}
