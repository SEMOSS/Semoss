package prerna.sablecc2.reactor.frame;

import prerna.algorithm.api.ITableDataFrame;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class LastUsedFrameReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		ITableDataFrame defaultFrame = null;
		// get the last frame
		SelectQueryStruct qs = this.insight.getLastQS(this.insight.getLastPanelId());
		if(qs != null) {
			defaultFrame = qs.getFrame();
		}
		if(defaultFrame == null) {
			SemossPixelException exception = new SemossPixelException(new NounMetadata("No frame currently exists", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(true);
			throw exception;
		}
		
		NounMetadata noun = new NounMetadata(defaultFrame, PixelDataType.FRAME, PixelOperationType.FRAME);
		return noun;
	}

}
