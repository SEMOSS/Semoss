package prerna.sablecc.expressions.sql;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;

import prerna.sablecc.AbstractReactor;
import prerna.sablecc.PKQLEnum;

public abstract class AbstractSqlExpression extends AbstractReactor {

	// this will store the base script
	// since there can be embedded scripts
	protected String baseScript;
	
	// this will store the join columns 
	// storing the base columns used in the expression
	protected String[] joinColumns;
	
	@Override
	public Iterator process() {
		String nodeStr = myStore.get(whoAmI).toString();

		// if this is wrapping an existing expression iterator
		if(myStore.get(nodeStr) instanceof H2SqlExpressionIterator) {
			((H2SqlExpressionIterator) myStore.get(nodeStr)).close();
		}
		
		// get the new base expression
		modExpression();
		
		// store it as the base script so the implementations
		// of the sql expressions can use it
		this.baseScript = myStore.get("MOD_" + whoAmI).toString();
		this.baseScript = baseScript.replace("[", "").replace("]", "");

		// since multiple math routines can be added together
		// need to get a unique set of values used in the join
		Vector<String> columns = (Vector <String>) myStore.get(PKQLEnum.COL_DEF);
		Set<String> joins = new HashSet<String>();
		joins.addAll(columns);
		
		this.joinColumns = joins.toArray(new String[]{});
		
		return null;
	}
	
}
