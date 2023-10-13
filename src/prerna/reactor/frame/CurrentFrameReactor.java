package prerna.reactor.frame;

import prerna.algorithm.api.ITableDataFrame;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class CurrentFrameReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		ITableDataFrame dm = (ITableDataFrame) this.insight.getDataMaker();
		if(dm == null) {
			SemossPixelException exception = new SemossPixelException(new NounMetadata("No frame currently exists", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(true);
			throw exception;
		}
		
		NounMetadata noun = new NounMetadata(dm, PixelDataType.FRAME, PixelOperationType.FRAME);
		return noun;
	}

}
