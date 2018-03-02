package prerna.sablecc2.reactor.qs.source;

import prerna.algorithm.api.ITableDataFrame;
import prerna.query.querystruct.QueryStruct2;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.qs.AbstractQueryStructReactor;

public class FrameReactor extends AbstractQueryStructReactor {
	
	public FrameReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FRAME.getKey()};
	}

	@Override
	protected QueryStruct2 createQueryStruct() {
		ITableDataFrame frame = getFrameInput();
		qs.setFrame(frame);
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
		return (ITableDataFrame) this.insight.getDataMaker();
	}
	
}
