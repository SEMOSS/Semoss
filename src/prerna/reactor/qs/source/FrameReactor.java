package prerna.reactor.qs.source;

import java.util.List;

import prerna.algorithm.api.ITableDataFrame;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.reactor.qs.AbstractQueryStructReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

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
			} else if(noun.getNounType() == PixelDataType.CONST_STRING) {
				// is it a variable?
				NounMetadata possibleFrameNoun = this.insight.getVarStore().get(noun.getValue() + "");
				if(possibleFrameNoun.getNounType() == PixelDataType.FRAME) {
					return (ITableDataFrame) possibleFrameNoun.getValue();
				}
			}
		}
		
		frameGrs = this.store.getNoun(PixelDataType.FRAME.toString());
		if(frameGrs != null && !frameGrs.isEmpty()) {
			NounMetadata noun = frameGrs.getNoun(0);
			if(noun.getNounType() == PixelDataType.FRAME) {
				return (ITableDataFrame) noun.getValue();
			} else if(noun.getNounType() == PixelDataType.CONST_STRING) {
				// is it a variable?
				NounMetadata possibleFrameNoun = this.insight.getVarStore().get(noun.getValue() + "");
				if(possibleFrameNoun.getNounType() == PixelDataType.FRAME) {
					return (ITableDataFrame) possibleFrameNoun.getValue();
				}
			}
		}

		List<NounMetadata> frameCur = this.curRow.getNounsOfType(PixelDataType.FRAME);
		if(frameCur != null && !frameCur.isEmpty()) {
			return (ITableDataFrame) frameCur.get(0).getValue();
		}

		List<String> frameVariableName = this.curRow.getAllStrValues();
		if(frameVariableName != null && !frameVariableName.isEmpty()) {
			NounMetadata possibleFrameNoun = this.insight.getVarStore().get(frameVariableName.get(0));
			if(possibleFrameNoun != null && possibleFrameNoun.getNounType() == PixelDataType.FRAME) {
				return (ITableDataFrame) possibleFrameNoun.getValue();
			}
		}
		
		if(this.insight != null) {
			return (ITableDataFrame) this.insight.getDataMaker();	
		}
		return null;
	}

}
