package prerna.reactor.frame;

import java.util.List;

import prerna.algorithm.api.ITableDataFrame;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class FrameTypeReactor extends AbstractReactor {
	
	public FrameTypeReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FRAME.getKey()};
	}

	@Override
	public NounMetadata execute() {
		ITableDataFrame frame = null;
		// see if a frame is passed in
		List<Object> frameInputs = this.curRow.getValuesOfType(PixelDataType.FRAME);
		if(frameInputs == null || frameInputs.isEmpty()) {
			frame = (ITableDataFrame) this.insight.getDataMaker();
		} else {
			// else, grab the default frame from the insight
			frame = (ITableDataFrame) frameInputs.get(0);
		}
		
		// if still can't find, throw an error
		if(frame == null) {
			throw new NullPointerException("No frame found");
		}
		
		// return the data maker name
		return new NounMetadata(frame.getFrameType().getTypeAsString(), PixelDataType.CONST_STRING);
	}

}
