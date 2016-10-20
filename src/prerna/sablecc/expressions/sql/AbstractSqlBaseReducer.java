package prerna.sablecc.expressions.sql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;

import prerna.ds.H2.H2Frame;
import prerna.sablecc.AbstractReactor;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLRunner.STATUS;

public abstract class AbstractSqlBaseReducer extends AbstractReactor {

	public AbstractSqlBaseReducer() {
		String[] thisReacts = { PKQLEnum.EXPR_TERM, PKQLEnum.DECIMAL, PKQLEnum.NUMBER, PKQLEnum.GROUP_BY, PKQLEnum.COL_DEF, PKQLEnum.MATH_PARAM};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.MATH_FUN;
	}
	
	/**
	 * This will generate the sql script for the routine
	 * @param tableName			The name of the sql table
	 * @param script			The script representing the column to get
	 * @param fitlers			The filters on the H2Frame
	 * @return					The full sql script to execute
	 */
	public abstract String process(H2Frame frame, String script);
	
	/**
	 * This will generate the sql script for the routine
	 * @param tableName			The name of the sql table
	 * @param script			The script representing the column to get
	 * @param groupByCols		The columns to group by in the sql query
	 * @param fitlers			The filters on the H2Frame
	 * @return					The full sql script to execute
	 */
	public abstract H2SqlExpressionIterator processGroupBy(H2Frame frame, String script, String[] groupByCols);

	
	@Override
	public Iterator process() {
		String nodeStr = myStore.get(whoAmI).toString();

		String[] existingGroups = null;
		// if this is wrapping an existing expression iterator
		if(myStore.get(whoAmI) instanceof H2SqlExpressionIterator) {
			existingGroups = ((H2SqlExpressionIterator) myStore.get(whoAmI)).getGroupColumns();
			((H2SqlExpressionIterator) myStore.get(whoAmI)).close();
		}
		
		// modify the expression to get the sql syntax
		modExpression();
		
		String script = myStore.get("MOD_" + whoAmI).toString();
		script = script.replace("[", "").replace("]", "");
		
		H2Frame h2Frame = (H2Frame)myStore.get("G");
		
		// get all the groups
		Vector<String> groupBys = (Vector <String>)myStore.get(PKQLEnum.COL_CSV);
		if(groupBys == null) {
			groupBys = new Vector<String>();
		}
		if(existingGroups != null) {
			for(String group : existingGroups) {
				groupBys.add(group);
			}
		}
		Set<String> groups = new HashSet<String>();
		groups.addAll(groupBys);
		
		if(!groups.isEmpty()){
			H2SqlExpressionIterator it = processGroupBy(h2Frame, script, groups.toArray(new String[]{}));
//			ResultSet rs = h2Frame.execQuery(sqlScript);
//			
//			// this is only here because this is what viz reactor expects
//			// TODO: when we get job ids, will be very happy to get rid of this
//			// annoying object
//			HashMap<HashMap<Object,Object>,Object> groupByHash = new HashMap<HashMap<Object,Object>,Object>();
//			
//			int numReturns = groupBys.size() + 1;
//			
//			try {
//				while(rs.next()) {
//					Object value = rs.getObject(1);
//					HashMap<Object, Object> groupMap = new HashMap<Object, Object>();
//					for(int i = 2; i <= numReturns; i++) {
//						groupMap.put(groupBys.get(i-2), rs.getObject(i));
//					}
//					
//					groupByHash.put(groupMap, value);
//				}
//			} catch (SQLException e) {
//				e.printStackTrace();
//			}
			myStore.put(nodeStr, it);
			myStore.put("STATUS",STATUS.SUCCESS);
		} else {
			String sqlScript = process(h2Frame, script);
			ResultSet rs = h2Frame.execQuery(sqlScript);
			Object result = null;
			try {
				rs.next();
				result = rs.getObject(1);
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
			myStore.put(nodeStr, result);
			myStore.put("STATUS",STATUS.SUCCESS);
		}
		
		return null;
	}
}
