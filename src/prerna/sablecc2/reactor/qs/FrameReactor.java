package prerna.sablecc2.reactor.qs;

import prerna.algorithm.api.ITableDataFrame;
import prerna.query.querystruct.QueryStruct2;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;

public class FrameReactor extends QueryStructReactor {
	
	public FrameReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FRAME.getKey()};
	}

	@Override
	QueryStruct2 createQueryStruct() {
		ITableDataFrame frame = getFrameInput();
		if(frame != null) {
			qs.setFrame(frame);
		}
		qs.setQsType(QueryStruct2.QUERY_STRUCT_TYPE.FRAME);
		return qs;
	}
	
	private ITableDataFrame getFrameInput() {
		if(this.curRow != null && this.curRow.size() > 0) {
			NounMetadata nounInput = this.curRow.getNoun(0);
			if(nounInput.getNounType() == PixelDataType.FRAME) {
				return (ITableDataFrame) nounInput.getValue();
			} else {
				// maybe this is a variable
				String varName = nounInput.getValue().toString();
				NounMetadata varNoun = this.planner.getVariableValue(varName);
				if(varNoun.getNounType() == PixelDataType.FRAME) {
					return (ITableDataFrame) varNoun.getValue();
				}
			}
		}
		return null;
	}
	
}
