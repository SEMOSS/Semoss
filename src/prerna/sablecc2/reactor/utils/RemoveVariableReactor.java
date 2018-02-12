package prerna.sablecc2.reactor.utils;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.h2.H2Frame;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.insights.AbstractInsightReactor;

public class RemoveVariableReactor extends AbstractInsightReactor{
	
	public RemoveVariableReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.VARIABLE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		String variableName = this.curRow.get(0).toString();
		NounMetadata noun = this.planner.removeVariable(variableName);
		boolean success = true;
		if(noun == null) {
			success = false;
		} else {
			PixelDataType nType = noun.getNounType();
			if(nType == PixelDataType.FRAME) {
				ITableDataFrame dm = (ITableDataFrame) noun.getValue();
				//TODO: expose a delete on the frame to hide this crap
				// drop the existing tables/connections if present
				if(dm instanceof H2Frame) {
					H2Frame frame = (H2Frame)dm;
					frame.dropTable();
					if(!frame.isInMem()) {
						frame.dropOnDiskTemporalSchema();
					}
				}
			}
		}
		return new NounMetadata(success, PixelDataType.BOOLEAN);
	}

}
