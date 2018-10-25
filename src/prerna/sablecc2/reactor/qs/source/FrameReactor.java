package prerna.sablecc2.reactor.qs.source;

import java.util.List;

import prerna.algorithm.api.ITableDataFrame;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.qs.AbstractQueryStructReactor;

public class FrameReactor extends AbstractQueryStructReactor {

	public FrameReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FRAME.getKey()};
	}

	@Override
	protected AbstractQueryStruct createQueryStruct() {
		ITableDataFrame frame = getFrameInput();
		qs.setFrame(frame);
		qs.setQsType(SelectQueryStruct.QUERY_STRUCT_TYPE.FRAME);
		return qs;
	}

	private ITableDataFrame getFrameInput() {
		GenRowStruct frameGrs = this.store.getNoun(this.keysToGet[0]);
		if(frameGrs != null && !frameGrs.isEmpty()) {
			NounMetadata noun = frameGrs.getNoun(0);
			if(noun.getNounType() == PixelDataType.FRAME) {
				return (ITableDataFrame) noun.getValue();
			}
		}

		List<NounMetadata> frameCur = this.curRow.getNounsOfType(PixelDataType.FRAME);
		if(frameCur != null && !frameCur.isEmpty()) {
			return (ITableDataFrame) frameCur.get(0).getValue();
		}

		if(this.insight != null) {
			return (ITableDataFrame) this.insight.getDataMaker();	
		}
		return null;
	}

}
