package prerna.sablecc;

import java.util.Iterator;

public class DataFrameDuplicatesReactor extends AbstractReactor {

	public DataFrameDuplicatesReactor() {
		String [] thisReacts = {PKQLEnum.COL_CSV};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.DATA_FRAME_DUPLICATES;
	}
	
	@Override
	public Iterator process() {
		// TODO Auto-generated method stub
		return null;
	}

}
