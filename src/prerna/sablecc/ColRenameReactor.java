package prerna.sablecc;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.algorithm.api.ITableDataFrame;
import prerna.sablecc.AbstractReactor;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLRunner.STATUS;
import prerna.sablecc.meta.ColAddMetadata;
import prerna.sablecc.meta.ColRenameMetadata;
import prerna.sablecc.meta.IPkqlMetadata;

public class ColRenameReactor extends AbstractReactor {

	public ColRenameReactor () {
		String[] thisReacts = { PKQLEnum.COL_DEF, PKQLEnum.COL_RENAME};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.COL_RENAME;
	}
	
	
	@Override
	public Iterator process() {
		//cleans the pqkl string
		modExpression();

		//get the Frame
		ITableDataFrame frame = (ITableDataFrame)myStore.get("G");
		Map<String, Set<String>> frameEdgeHash = frame.getEdgeHash();
		
		String[] originalColHeaders = frame.getColumnHeaders();
		
		//gets a list of all the column names
		List<String> colList = (List<String>)myStore.get(PKQLEnum.COL_DEF);
		String oldColName = colList.get(0);
		String newColName = colList.get(1);
		if(Arrays.asList(originalColHeaders).contains(newColName) || oldColName.equals(newColName)){
			return null;
		}

		//if the passed in old name is contained in the list of column names then change to new name
			Set<String> colSet = frameEdgeHash.keySet();
			if(colSet.contains(oldColName)) {
				//rename column on the meta and instance level
				frame.renameColumn(oldColName, newColName);
			}
		
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
