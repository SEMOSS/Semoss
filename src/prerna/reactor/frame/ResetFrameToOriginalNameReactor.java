package prerna.reactor.frame;

import prerna.algorithm.api.ITableDataFrame;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ResetFrameToOriginalNameReactor extends AbstractFrameReactor {

	public ResetFrameToOriginalNameReactor() {
		this.keysToGet = new String[]{ ReactorKeysEnum.FRAME.getKey() } ;
	}

	@Override
	public NounMetadata execute() {
		ITableDataFrame frame = getFrameDefaultLast();
		// reset the name back to the original name
		if(frame.getName().equals(frame.getOriginalName())) {
			return new NounMetadata(true, PixelDataType.BOOLEAN);
		}
		frame.setName(frame.getOriginalName());
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}
	
}
