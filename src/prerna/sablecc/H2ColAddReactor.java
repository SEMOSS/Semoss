package prerna.sablecc;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.ExpressionIterator;
import prerna.ds.TinkerMetaHelper;
import prerna.ds.H2.H2Frame;
import prerna.sablecc.PKQLRunner.STATUS;
import prerna.sablecc.meta.ColAddMetadata;
import prerna.sablecc.meta.IPkqlMetadata;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Utility;

public class H2ColAddReactor extends AbstractReactor {

	private static final Logger LOGGER = LogManager.getLogger(H2ColAddReactor.class.getName());

	public H2ColAddReactor() {
		String[] thisReacts = { PKQLEnum.COL_DEF, PKQLEnum.COL_DEF + "_1", PKQLEnum.API, PKQLEnum.EXPR_TERM };
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.COL_ADD;
	}

	@Override
	public Iterator process() {
		// TODO Auto-generated method stub
		// I need to take the col_def
		// and put it into who am I
		modExpression();
		H2Frame frame = (H2Frame) myStore.get("G");

		// new column name
		String newCol = (String) myStore.get(PKQLEnum.COL_DEF + "_1");
		LOGGER.info("Running colAdd to create column: " + newCol);

		// so whatever it is that we are adding
		// will be passed in here as an expression term
		// this could:
		// 1) a string containing an expression
		// 2) an iterator which we need to add
		// 3) a map when there is a group by
		// 4) a ExpressionIterator that is using stupid groovy :(
		
		// expr is the term
		// and the value will be what that term points to inside myStore
		String expr = (String) myStore.get(PKQLEnum.EXPR_TERM);
		LOGGER.info("Running colAdd for expression: " + expr);
		Object value = myStore.get(expr);

		// joinColumns
		// when there are multiple math routines, we need to make sure we get a unique set of join columns
		Vector<String> cols = (Vector<String>) myStore.get(PKQLEnum.COL_DEF);
		cols.remove(newCol);
		Set<String> uniqueCols = new HashSet<String>();
		uniqueCols.addAll(cols);
		String[] joinCols = uniqueCols.toArray(new String[]{});

		// this corresponds to case 3
		// we processed something and group information is processed
		if(value instanceof Map) 
		{
			addColumnUsingMap(frame, (Map<Map<Object, Object>, Object>) value, newCol, joinCols);
		} 

		else if(value instanceof H2SqlExpressionIterator) 
		{
			addColumnUsingExpression(frame, (H2SqlExpressionIterator) value, newCol, joinCols);
		} 

		// ugh... dont like this...
		// need to come back and see if we can modify all the old reactors
		// that is utilizing the groovy
		else if(value instanceof ExpressionIterator) 
		{
			processIt((ExpressionIterator) value, frame, joinCols, newCol);
		}
		
		else 
		{
			// sometimes, the script has not undergone modExpresssion
			if (value == null) {
				value = modExpression(expr);
			}
			
			H2SqlExpressionIterator it = new H2SqlExpressionIterator(frame, value.toString(), newCol, joinCols);
			addColumnUsingExpression(frame, it, newCol, joinCols);
		}
		
		frame.updateDataId();
		
		myStore.put("RESPONSE", STATUS.SUCCESS.toString());
		myStore.put("STATUS", STATUS.SUCCESS);
		
		return null;
	}

	private void addColumnUsingExpression(H2Frame frame, H2SqlExpressionIterator it, String newColumn, String[] joinColumns) {
		// since we do not know the type of the new column we are adding
		// we determine it based on the first value that is returned
		PreparedStatement ps = null;
		
		// get the values returned
		String[] columnsToGet = it.getHeaders();
		
		// generate a mapping to get the correct indices
		// this is os we do not need to create a map and can use simple arrays
		int[] indices = new int[columnsToGet.length];
		
		// we us make index 0 the newColumn
		// and then make each one in order
		if(it.getAliasForScript() != null) {
			// if the alias is set, get there just in case it was defined outside of the colAddReactor
			// to be a random value
			indices[0] = ArrayUtilityMethods.arrayContainsValueAtIndexIgnoreCase(columnsToGet, it.getAliasForScript());	
		} else {
			indices[0] = ArrayUtilityMethods.arrayContainsValueAtIndexIgnoreCase(columnsToGet, newColumn);
		}
		// NOTE: we follow the assumption that the columnsToGet is the combination of the new column and the join columns
		for(int i = 1; i < columnsToGet.length; i++) {
			indices[i] = ArrayUtilityMethods.arrayContainsValueAtIndexIgnoreCase(columnsToGet, joinColumns[i-1]);
		}
		
		// doing this update in batches
		final int BATCH_SIZE = 5000;
		int count = 0;

		try {
			// we need to loop through and add all the data
			while(it.hasNext()) {
				Object[] values = it.next();

				Object newVal = values[indices[0]];
				
				// here we use the first value that is defined to create the first column
				// since we need the type
				if(ps == null) {
					// here we create the new column
					Object[] newType = Utility.findTypes(newVal.toString());
					String type = "";
					type = newType[0].toString();
					Map<String, String> dataType = new HashMap<>(1);
					dataType.put(newColumn, type);
					frame.connectTypes(joinColumns, newColumn, dataType);
					frame.setDerivedColumn(newColumn, true);

					// and here we create the prepared statement 
					String[] newColsArr = new String[]{newColumn};
					ps = frame.createUpdatePreparedStatement(newColsArr, joinColumns);
				}

				// things to note:
				// the way the prepared statement is created
				// we need to set the value in the prepared statement in the same order
				// as we passed in the values
				// so first need to add the newValue and then joinColumns in order

				// setting the value to set
				ps.setObject(1, newVal);
				for(int i = 0; i < joinColumns.length; i++) {
					// setting the where clause in the sql statement
					ps.setObject(i+2, values[indices[i+1]]);
				}

				// add it
				ps.addBatch();

				// batch commit based on size
				if (++count % BATCH_SIZE == 0) {
					ps.executeBatch();
				}
			}

			if(ps != null) {
				// well, we are done looping through now
				ps.executeBatch(); // insert any remaining records
				ps.close();
			} else {
				LOGGER.error("DID NOT ADD ANY INFORMATION INTO COLUMN... CHECK QUERY BEING EXECUTED");
			}
			
		} catch(SQLException e) {
			e.printStackTrace();
		}

	}

	/**
	 * Adding new column utilizing 
	 * @param frame
	 * @param dataToAdd
	 * @param newColumn
	 * @param joinColumns
	 */
	public void addColumnUsingMap(H2Frame frame, Map<Map<Object, Object>, Object> dataToAdd, String newColumn, String[] joinColumns) {
		// since we do not know the type of the new column we are adding
		// we determine it based on the first value that is returned
		PreparedStatement ps = null;

		// doing this update in batches
		final int BATCH_SIZE = 5000;
		int count = 0;

		try {
			// we need to loop through and add all the data
			for(Map<Object, Object> groupData : dataToAdd.keySet()) {

				Object newVal = dataToAdd.get(groupData);

				// here we use the first value that is defined to create the first column
				// since we need the type
				if(ps == null) {
					// here we create the new column
					Object[] newType = Utility.findTypes(newVal.toString());
					String type = "";
					type = newType[0].toString();
					Map<String, String> dataType = new HashMap<>(1);
					dataType.put(newColumn, type);
					frame.connectTypes(joinColumns, newColumn, dataType);
					frame.setDerivedColumn(newColumn, true);

					// and here we create the prepared statement 
					String[] newColsArr = new String[]{newColumn};
					ps = frame.createUpdatePreparedStatement(newColsArr, joinColumns);
				}

				// things to note:
				// the way the prepared statement is created
				// we need to set the value in the prepared statement in the same order
				// as we passed in the values
				// so first need to add the newValue and then joinColumns in order

				// setting the value to set
				ps.setObject(1, newVal);
				for(int i = 0; i < joinColumns.length; i++) {
					// setting the where clause in the sql statement
					ps.setObject(i+2, groupData.get(joinColumns[i]));
				}

				// add it
				ps.addBatch();

				// batch commit based on size
				if (++count % BATCH_SIZE == 0) {
					ps.executeBatch();
				}
			}

			// well, we are done looping through now
			ps.executeBatch(); // insert any remaining records
			ps.close();

		} catch(SQLException e) {
			e.printStackTrace();
		}
	}
	
	/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// this is the old expression iterator we might want to look at finding a way to replace all of them
	
	private void processIt(Iterator it, ITableDataFrame frame, String[] joinCols, String newCol) {
		if (it.hasNext()) {

			boolean addMetaData = true;

			if (joinCols.length > 1) { // multicolumn join
				String primKeyName = TinkerMetaHelper.getPrimaryKey(joinCols);
				while (it.hasNext()) {
					HashMap<String, Object> row = new HashMap<String, Object>();
					Object newVal = it.next();
					Object[] values = new Object[joinCols.length];
					if ((newVal instanceof List) && ((List) newVal).size() == 1)
						row.put(newCol, ((List) newVal).get(0));
					else {
						row.put(newCol, newVal);
					}
					for (int i = 0; i < joinCols.length; i++) {
						if (it instanceof ExpressionIterator) {
							Object rowVal = ((ExpressionIterator) it).getOtherBindings().get(joinCols[i]);
							row.put(joinCols[i], rowVal);
							values[i] = rowVal;
						}
					}
					row.put(primKeyName, TinkerMetaHelper.getPrimaryKey(values));

					if (addMetaData) {
						Object[] newType = Utility.findTypes(newVal.toString());
						String type = "";
						type = newType[0].toString();
						Map<String, String> dataType = new HashMap<>(1);
						dataType.put(newCol, type);
						frame.connectTypes(joinCols, newCol, dataType);
						frame.setDerivedColumn(newCol, true);
						addMetaData = false;
					}

					frame.addRelationship(row);
				}
				myStore.put("STATUS", STATUS.SUCCESS);
			} else {
				while (it.hasNext()) {
					HashMap<String, Object> row = new HashMap<String, Object>();
					Object newVal = it.next();
					if ((newVal instanceof List) && ((List) newVal).size() == 1)
						row.put(newCol, ((List) newVal).get(0));
					else {
						row.put(newCol, newVal);
					}
					for (int i = 0; i < joinCols.length; i++) {
						if (it instanceof ExpressionIterator) {
							row.put(joinCols[i], ((ExpressionIterator) it).getOtherBindings().get(joinCols[i]));
						}
					}

					if (addMetaData) {
						Object[] newType = Utility.findTypes(newVal.toString());
						String type = "";
						type = newType[0].toString();
						Map<String, String> dataType = new HashMap<>(1);
						dataType.put(newCol, type);
						frame.connectTypes(joinCols, newCol, dataType);
						frame.setDerivedColumn(newCol, true);
						addMetaData = false;
					}
					frame.addRelationship(row);
				}
				myStore.put("STATUS", STATUS.SUCCESS);
			}
		} else {
			myStore.put("STATUS", STATUS.ERROR);
		}
	}

	/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	
	
	public IPkqlMetadata getPkqlMetadata() {
		String expr =  (String) myStore.get(PKQLEnum.EXPR_TERM);
		//remove ()'s
		expr.trim();
		if(expr.charAt(0) == '(') {
			expr = expr.substring(1, expr.length()-1);
		}
		ColAddMetadata metadata = new ColAddMetadata((String) myStore.get("COL_DEF_1"),expr);
		metadata.setPkqlStr((String) myStore.get(PKQLEnum.COL_ADD));
		return metadata;
	}

}
