package prerna.sablecc;

import java.util.Iterator;
import java.util.List;

import prerna.algorithm.api.ITableDataFrame;
import prerna.sablecc.PKQLRunner.STATUS;
import prerna.sablecc.meta.ColRenameMetadata;
import prerna.sablecc.meta.IPkqlMetadata;
import prerna.util.ArrayUtilityMethods;

public class ColRenameReactor extends AbstractReactor {

	public ColRenameReactor () {
		String[] thisReacts = { PKQLEnum.COL_DEF, PKQLEnum.COL_RENAME};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.COL_RENAME;
	}
	
	
	@Override
	public Iterator process() {
		//get the Frame
		ITableDataFrame frame = (ITableDataFrame) myStore.get("G");
		String[] origHeaders = frame.getColumnHeaders();
		
		//gets a list of all the column names
		List<String> colList = (List<String>)myStore.get(PKQLEnum.COL_DEF);
		String oldColName = colList.get(0);
		boolean validHeader = ArrayUtilityMethods.arrayContainsValue(origHeaders, oldColName);
		if(!validHeader) {
			myStore.put("STATUS",STATUS.ERROR);
			throw new IllegalArgumentException("Invalid header. " + oldColName + " does not exist as a header");
		}
		
		String newColName = colList.get(1);
		// add an alias on the header
//		frame.renameColumn(oldColName, newColName);

		// update the data id so FE knows data has been changed
		frame.updateDataId();
		myStore.put("STATUS",STATUS.SUCCESS);
		
		return null;
	}
	
	@Override
	public IPkqlMetadata getPkqlMetadata() {
		String expr =  (String) myStore.get(PKQLEnum.EXPR_TERM);
		//remove ()'s
		expr.trim();
		if(expr.charAt(0) == '(') {
			expr = expr.substring(1, expr.length()-1);
		}
		List<String> col = (List<String>) myStore.get(PKQLEnum.COL_DEF);
		ColRenameMetadata metadata = new ColRenameMetadata((String) col.get(0), (String) col.get(1), expr);
		metadata.setPkqlStr((String) myStore.get(PKQLEnum.COL_RENAME));
		return metadata;
	}

}
