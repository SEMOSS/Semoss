package prerna.sablecc;

import prerna.sablecc.meta.IPkqlMetadata;

public abstract class DataFrameDuplicatesReactor extends AbstractReactor {

	public DataFrameDuplicatesReactor() {
		String [] thisReacts = {PKQLEnum.COL_CSV};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.DATA_FRAME_DUPLICATES;
	}
	
	@Override
	public IPkqlMetadata getPkqlMetadata() {
		// TODO Auto-generated method stub
		return null;
	}

}
