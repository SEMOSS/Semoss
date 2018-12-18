package prerna.sablecc2.reactor.export;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.TaskUtility;
import prerna.sablecc2.reactor.AbstractReactor;

public class EmptyDataReactor extends AbstractReactor {

	public EmptyDataReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.VALUE.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		Object value = getValue();
		boolean noData = TaskUtility.noData(value);
		return new NounMetadata(noData, PixelDataType.BOOLEAN);
	}
	
	private Object getValue() {
		GenRowStruct grs = this.store.getNoun(this.keysToGet[0]);
		if(grs != null && !grs.isEmpty()) {
			return grs.get(0);
		}
		
		grs = this.store.getNoun(PixelDataType.FORMATTED_DATA_SET.toString());
		if(grs != null && !grs.isEmpty()) {
			return grs.get(0);
		}
		
		if(this.curRow != null && !this.curRow.isEmpty()) {
			return this.curRow.get(0);
		}
		
		throw new IllegalArgumentException("No data passed in");
	}

}
