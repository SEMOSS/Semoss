package prerna.sablecc2.reactor.frame;

import prerna.algorithm.api.ITableDataFrame;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SetCurrentFrameReactor extends AbstractFrameReactor{

	public SetCurrentFrameReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FRAME.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		ITableDataFrame dm = getFrame();
		if(dm == null) {
			throw new IllegalArgumentException("Could not find frame to set");
		}
		// set the new main dm for the insight
		this.insight.setDataMaker(dm);
		NounMetadata noun = new NounMetadata(dm, PixelDataType.FRAME, PixelOperationType.FRAME);
		noun.addAdditionalReturn(new NounMetadata("New frame has been set", PixelDataType.CONST_STRING, PixelOperationType.SUCCESS));
		return noun;
	}
}
