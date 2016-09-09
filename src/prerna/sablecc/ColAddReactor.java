package prerna.sablecc;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.ExpressionIterator;
import prerna.ds.TinkerMetaHelper;
import prerna.sablecc.PKQLRunner.STATUS;
import prerna.sablecc.meta.ColAddMetadata;
import prerna.sablecc.meta.IPkqlMetadata;
import prerna.util.Utility;

public class ColAddReactor extends AbstractReactor {

	Hashtable<String, String[]> values2SyncHash = new Hashtable<String, String[]>();

	public ColAddReactor() {
		// these are the input columns - there is also expr Term which I will
		// come to shortly
		String[] thisReacts = { PKQLEnum.COL_DEF, PKQLEnum.COL_DEF + "_1", PKQLEnum.API, PKQLEnum.EXPR_TERM };
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.COL_ADD;

		// this is the point where I specify what are the child values required
		// for various input
		String[] dataFromExpr = { PKQLEnum.COL_DEF };
		values2SyncHash.put(PKQLEnum.EXPR_TERM, dataFromExpr);

		String[] dataFromApi = { PKQLEnum.COL_CSV };
		values2SyncHash.put(PKQLEnum.API, dataFromApi);

		// setting pkqlMetaData
		String title = "Add a new column";
		String pkqlCommand = "col.add(c:newCol,expression);";
		String description = "Adds a new column named newCol, setting each cell to the result of the expression";
		boolean showMenu = true;
		boolean pinned = true;
		super.setPKQLMetaData(title, pkqlCommand, description, showMenu, pinned);
		super.setPKQLMetaDataInput();
	}

	@Override
	public Iterator process() {
		// TODO Auto-generated method stub
		// I need to take the col_def
		// and put it into who am I
		modExpression();
		String nodeStr = (String) myStore.get(whoAmI);
		System.out.println("My Store on COL CSV " + myStore);
		ITableDataFrame frame = (ITableDataFrame) myStore.get("G");

		String[] joinCols = null;
		Iterator it = null;
		String newCol = (String) myStore.get(PKQLEnum.COL_DEF + "_1");

		// ok this came in as an expr term
		// I need to do the iterator here
		System.err.println(myStore.get(PKQLEnum.EXPR_TERM));

		// ok.. so it would be definitely be cool to pass this to an expr script
		// right now and do the op
		// however I dont have this shit
		String expr = (String) myStore.get(PKQLEnum.EXPR_TERM);

		Vector<String> cols = (Vector<String>) myStore.get(PKQLEnum.COL_DEF);
		// col def of the parent will have all of the col defs of the children
		// need to remove the new column as it doesn't exist yet
		cols.remove(newCol);
		it = getTinkerData(cols, frame, false);
		joinCols = convertVectorToArray(cols);
		Object value = myStore.get(expr);
		if (value == null)
			value = myStore.get(PKQLEnum.API);

		// if(value instanceof ColAddIterator) {
		// ((ColAddIterator)value).updateNewColName(newCol);
		// ((ColAddIterator)value).processIterator(frame);
		// } else
		if (value instanceof Iterator) {
			it = (ExpressionIterator) value;
			processIt(it, frame, joinCols, newCol);
		} else if (value instanceof Map) { // this will be the case when we are
											// adding group by data
			// this map is in the form { {groupedColName=groupedColValue} =
			// calculatedValue }
			Map vMap = (Map) value;
			boolean addMetaData = true;
			for (Object mapKey : vMap.keySet()) {
				Vector<String> cols2 = new Vector<String>();
				for (Object key : ((Map) mapKey).keySet()) {
					cols2.add(key + "");
				}
				String[] joinColss = convertVectorToArray(cols2);

				Map mk = (Map) mapKey;
				Map<String, Object> row = new HashMap<>();
				for (Object key : mk.keySet()) {
					row.put(key + "", mk.get(key));
				}

				Object newVal = vMap.get(mapKey);
				row.put(newCol, vMap.get(mapKey));

				if (addMetaData) {
					Object[] newType = Utility.findTypes(newVal.toString());
					String type = "";
					type = newType[0].toString();
					Map<String, String> dataType = new HashMap<>(1);
					dataType.put(newCol, type);
					frame.connectTypes(joinColss, newCol, dataType);
					frame.setDerivedColumn(newCol, true);
					addMetaData = false;
				}

				frame.addRelationship(row, row);
			}
		} else {
			if (value == null) {
				value = modExpression(expr); // expr doesn't get modded
												// initially since its grabbed
												// separately from whoAmI. Need
												// to mod it here.
			}
			it = new ExpressionIterator(it, joinCols, value.toString());
			processIt(it, frame, joinCols, newCol);
		}
		myStore.put("RESPONSE", STATUS.SUCCESS.toString());
		myStore.put("STATUS", STATUS.SUCCESS);

		// update the data id so FE knows data has been changed
		frame.updateDataId();

		return null;
	}

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

					frame.addRelationship(row, row);
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
					frame.addRelationship(row, row);
				}
				myStore.put("STATUS", STATUS.SUCCESS);
			}
		} else {
			myStore.put("STATUS", STATUS.ERROR);
		}
	}

	// gets all the values to synchronize for this
	public String[] getValues2Sync(String input) {
		return values2SyncHash.get(input);
	}

	////////////// setting the values for PKQL JSON for FE//////////////////////

	/*
	 * private List<HashMap<String, Object>> populatePKQLMetaDataInput(){
	 * List<HashMap<String, Object>> input = new ArrayList<HashMap<String,
	 * Object>>(); HashMap<String, Object> inputMap = new HashMap<String,
	 * Object>(); Object restrictions = new Object(); //first variable in PKQL
	 * inputMap.put("label", "New Column Name"); inputMap.put("varName",
	 * "c:newCol"); inputMap.put("dataType", "text"); inputMap.put("type",
	 * "dropdown"); inputMap.put("restrictions", restrictions);
	 * inputMap.put("source", ""); input.add(inputMap);
	 * 
	 * //second variable in PKQL inputMap = new HashMap<String, Object>();
	 * inputMap.put("label", "New Column Value"); inputMap.put("varName",
	 * "(expression)"); inputMap.put("dataType", "expression");
	 * inputMap.put("type", "dropdown"); inputMap.put("restrictions",
	 * restrictions); inputMap.put("source", ""); input.add(inputMap); return
	 * input; }
	 */

	/*
	 * private List<HashMap<String, Object>> populatePKQLMetaDataInput(){
	 * List<HashMap<String, Object>> input = new ArrayList<HashMap<String,
	 * Object>>(); //HashMap<String, Object> inputMap = new HashMap<String,
	 * Object>(); //Object restrictions = new Object();
	 * 
	 * for(String var: this.whatIReactTo){ //create a Utility method and pass
	 * whoAmI and whatIReactTo[i]/var input.add(Utility.getPKQLInputVar(var,
	 * this.whoAmI)); } //first variable in PKQL inputMap.put("label",
	 * "New Column Name"); inputMap.put("varName", "c:newCol");
	 * inputMap.put("dataType", "text"); inputMap.put("type", "dropdown");
	 * inputMap.put("restrictions", restrictions); inputMap.put("source", "");
	 * input.add(inputMap);
	 * 
	 * //second variable in PKQL inputMap = new HashMap<String, Object>();
	 * inputMap.put("label", "New Column Value"); inputMap.put("varName",
	 * "(expression)"); inputMap.put("dataType", "expression");
	 * inputMap.put("type", "dropdown"); inputMap.put("restrictions",
	 * restrictions); inputMap.put("source", ""); input.add(inputMap); return
	 * input; }
	 */

	/*
	 * private HashMap<String, Object> populatePKQLMetaDataConsole(){
	 * 
	 * HashMap<String, Object> console = new HashMap<String, Object>(); String[]
	 * groups = null; Object buttonClass = new Object(); Object buttonActions =
	 * new Object();
	 * 
	 * console.put("name", "Console Name"); console.put("groups", groups);
	 * console.put("buttonContentLong", ""); console.put("buttonContent", "");
	 * console.put("buttonTitle", ""); console.put("buttonClass", buttonClass);
	 * console.put("buttonActions", buttonActions); return console; }
	 */

}
