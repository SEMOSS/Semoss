package prerna.sablecc;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.h2.H2Frame;
import prerna.sablecc.meta.ColUnfilterMetadata;
import prerna.sablecc.meta.IPkqlMetadata;

public class ColUnfilterReactor extends AbstractReactor {

	public ColUnfilterReactor() {
		String[] thisReacts = { PKQLEnum.COL_DEF }; // these are the input
													// columns - there is also
													// expr Term which I will
													// come to shortly
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.UNFILTER_DATA;

		// setting pkqlMetaData
//		String title = "Unfilter data in a column";
//		String pkqlCommand = "col.unfilter(c:col1);";
//		String description = "Unfilters the data in the selected column";
//		boolean showMenu = true;
//		boolean pinned = true;
//		super.setPKQLMetaData(title, pkqlCommand, description, showMenu, pinned);
//		super.setPKQLMetaDataInput();
	}

	@Override
	public Iterator process() {
		// I need to take the col_def
		// and put it into who am I
		modExpression();
		String nodeStr = (String) myStore.get(whoAmI);
		// System.out.println("My Store on COL CSV " + myStore);

		ITableDataFrame frame = (ITableDataFrame) myStore.get("G");

		Vector<String> column = (Vector<String>) myStore.get(PKQLEnum.COL_DEF);

		for (String c : column) {
			if(frame instanceof H2Frame) {
				if(((H2Frame)frame).isJoined()) {
					((H2Frame) frame).getJoiner().unfilter(frame, c);
				} else {
					frame.unfilter(c);
				}
			} else {
				frame.unfilter(c);
			}
			myStore.put("STATUS", PKQLRunner.STATUS.SUCCESS);
			myStore.put("FILTER_COLUMN", c);
		}

		// update the data id so FE knows data has been changed
		frame.updateDataId();

		return null;
	}

	public IPkqlMetadata getPkqlMetadata() {
		ColUnfilterMetadata metadata = new ColUnfilterMetadata((String) myStore.get("FILTER_COLUMN"));
		metadata.setPkqlStr((String) myStore.get(PKQLEnum.UNFILTER_DATA));
		return metadata;
	}
}
