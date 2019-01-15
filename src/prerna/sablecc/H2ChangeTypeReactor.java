package prerna.sablecc;

import java.util.Iterator;
import java.util.List;

import prerna.ds.h2.H2Frame;
import prerna.sablecc.PKQLRunner.STATUS;

public class H2ChangeTypeReactor extends DataframeChangeTypeReactor {

	// abstract layer handles stuff
	// need to make sure i define 3 things that sit in the abstract
	// 1) the column
	// 2) the old type
	// 3) the new type
	
	@Override
	public Iterator process() {
		H2Frame frame = (H2Frame) myStore.get("G");
		String tableName = frame.getName();
		
		// column name and new type are passed in the pkql
		this.columnName = ((List<String>) myStore.get(PKQLEnum.COL_DEF)).get(0);
		this.newType = (String) myStore.get(PKQLEnum.WORD_OR_NUM);
		// get the old type from the frame
		this.oldType = frame.getMetaData().getHeaderTypeAsString(tableName + "__" + this.columnName, tableName);
		frame.getMetaData().modifyDataTypeToProperty(tableName + "__" + this.columnName, tableName, this.newType);
		
		myStore.put("STATUS", STATUS.SUCCESS);
		return null;
	}

}
