package prerna.sablecc.expressions.sql;

import java.util.Iterator;
import java.util.Vector;

import prerna.ds.H2.H2Frame;
import prerna.sablecc.AbstractReactor;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.expressions.sql.builder.SqlBuilder;
import prerna.sablecc.expressions.sql.builder.SqlColumnSelector;

public abstract class AbstractSqlExpression extends AbstractReactor {

	// this contains the sql builder to modify
	protected SqlBuilder builder;
	
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
		if(myStore.get("TERM") instanceof SqlBuilder) {
			this.builder = (SqlBuilder) myStore.get("TERM");
		} else {
			// the input has to be a set of column as the starting point
			Vector<String> columns = (Vector<String>) myStore.get(PKQLEnum.COL_DEF);
			
			this.builder = new SqlBuilder(h2Frame);
			for(String col : columns) {
				SqlColumnSelector selector = new SqlColumnSelector(h2Frame, col);
				this.builder.addSelector(selector);
			}
		}
		
		return null;
	}
	
}
