package prerna.sablecc;

import prerna.sablecc.meta.DataframeChangeTypeMetadata;
import prerna.sablecc.meta.IPkqlMetadata;

public abstract class DataframeChangeTypeReactor extends AbstractReactor {

	protected String columnName;
	protected String newType;
	protected String oldType;
	
	public DataframeChangeTypeReactor() {
		String [] thisReacts = {PKQLEnum.COL_DEF, PKQLEnum.WORD_OR_NUM};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.DATA_FRAME_CHANGE_TYPE;
	}
	
	@Override
	public IPkqlMetadata getPkqlMetadata() {
		return new DataframeChangeTypeMetadata(columnName, newType, oldType);
	}
}
