package prerna.sablecc;

import java.util.Iterator;
import java.util.List;

import prerna.ds.h2.H2Frame;
import prerna.sablecc.PKQLRunner.STATUS;
import prerna.util.Utility;

public class H2DataframeChangeTypeReactor extends DataframeChangeTypeReactor {

	// abstract layer handles stuff
	// need to make sure i define 3 things that sit in the abstract
	// 1) the column
	// 2) the old type
	// 3) the new type
	
	@Override
	public Iterator process() {
		H2Frame frame = (H2Frame) myStore.get("G");
		
		// column name and new type are passed in the pkql
		this.columnName = ((List<String>) myStore.get(PKQLEnum.COL_DEF)).get(0);
		this.newType = (String) myStore.get(PKQLEnum.WORD_OR_NUM);
		// get the old type from the frame
		this.oldType = Utility.convertDataTypeToString(frame.getDataType(this.columnName));
		frame.changeDataType(columnName, newType);
		
		myStore.put("STATUS", STATUS.SUCCESS);
		return null;
	}

}
