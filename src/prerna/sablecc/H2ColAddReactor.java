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
import prerna.sablecc.expressions.sql.H2SqlExpressionIterator;
import prerna.sablecc.expressions.sql.builder.SqlColumnSelector;
import prerna.sablecc.expressions.sql.builder.SqlExpressionBuilder;
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
		LOGGER.info("RUNNING COL ADD FOR EXPRESSION : " + expr);
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

		else if(value instanceof SqlExpressionBuilder) 
		{
			SqlExpressionBuilder builder = (SqlExpressionBuilder) value;
			List<String> groups = builder.getGroupByColumns();
			if(groups == null || groups.isEmpty()) {
				// if no groups
				// use the existing columns to join on
				for(String joinCol : joinCols) {
					SqlColumnSelector selector = new SqlColumnSelector(frame, joinCol);
					builder.addSelector(selector);
				}
			} else {
				// use the group columns to join on
				for(String group : groups) {
					SqlColumnSelector selector = new SqlColumnSelector(frame, group);
					builder.addSelector(selector);
				}
			}
			addColumnUsingExpression(frame, (SqlExpressionBuilder) value, newCol);
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
			System.out.println("WHAT PKQL GOES HERE!!!!");
			System.out.println("WHAT PKQL GOES HERE!!!!");
			System.out.println("WHAT PKQL GOES HERE!!!!");
			System.out.println("WHAT PKQL GOES HERE!!!!");

			// sometimes, the script has not undergone modExpresssion
//			if (value == null) {
//				value = modExpression(expr);
//			}
//			
//			SqlBuilder builder = new SqlBuilder(frame);
//			
//			
//			for(String joinCol : joinCols) {
//				SqlColumnSelector selector = new SqlColumnSelector(frame, joinCol);
//				builder.addSelector(selector);
//			}
//			
//			// no group by columns here
//			H2SqlExpressionIterator it = new H2SqlExpressionIterator(frame, exprList, newColList, joinCols, null);
//			addColumnUsingExpression(frame, it, newCol, joinCols);
		}
		
		LOGGER.info("DONE RUNNING COL ADD");

		frame.updateDataId();
		
		myStore.put("RESPONSE", STATUS.SUCCESS.toString());
		myStore.put("STATUS", STATUS.SUCCESS);
		
		return null;
	}

	private void addColumnUsingExpression(H2Frame frame, SqlExpressionBuilder builder, String newColName) {
		String[] headers = frame.getColumnHeaders();
		
		// right now, even though the expression iterator
		// can handle multiple columns to get
		// col add syntax only allows for one
		
		// drop any index for faster updating
		Set<String> colsWithIndex = frame.getColumnsWithIndexes();
		for(String col : colsWithIndex) {
			frame.removeColumnIndex(col);
		}
		
		// since we do not know the type of the new column we are adding
		// we determine it based on the first value that is returned
		PreparedStatement ps = null;
		
		// get the values returned
		List<String> columnsToGet = builder.getSelectorNames();
		List<String> joins = new Vector<String>();
		for(String col : columnsToGet) {
			if(ArrayUtilityMethods.arrayContainsValue(headers, col)) {
				joins.add(col);
			}
		}
		String[] joinColumns = joins.toArray(new String[]{});
		
		// NOTE: FUNDAMENTAL ASSUMPTION THAT WE ARE ONLY ADDING IN A SINGLE COLUMN
		// FIRST OUTPUT IN SQL QUERY WILL RETURN THE COLUMN TO ADD
		// EVERYTHING ELSE IS A JOIN COLUMN
		
		H2SqlExpressionIterator it = new H2SqlExpressionIterator(builder);
		
		// doing this update in batches
		final int BATCH_SIZE = 5000;
		int count = 0;

		try {
			// we need to loop through and add all the data
			while(it.hasNext()) {
				Object[] values = it.next();

				Object newVal = values[0];
				
				// here we use the first value that is defined to create the first column
				// since we need the type
				if(ps == null) {
					// here we create the new column
					Object[] newType = Utility.findTypes(newVal.toString());
					String type = "";
					type = newType[0].toString();
					Map<String, String> dataType = new HashMap<>(1);
					dataType.put(newColName, type);
					frame.connectTypes(joinColumns, newColName, dataType);
					frame.setDerivedColumn(newColName, true);

					// and here we create the prepared statement 
					String[] newColsArr = new String[]{newColName};
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
					ps.setObject(i+2, values[i+1]);
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
		
		
		// add back index
		for(String col : colsWithIndex) {
			frame.addColumnIndex(col);
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

				// here we use the first value that is defined to creat)e the first column
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
		expr = expr.trim();
		if(expr.charAt(0) == '(') {
			expr = expr.substring(1, expr.length()-1);
		}
		ColAddMetadata metadata = new ColAddMetadata((String) myStore.get("COL_DEF_1"),expr);
		metadata.setPkqlStr((String) myStore.get(PKQLEnum.COL_ADD));
		return metadata;
	}

}
