package prerna.reactor;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class ParallelPixelRunReactor extends AbstractReactor{

	public ParallelPixelRunReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PIXEL.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String pixelToRun = this.keyValue.get(this.keysToGet[0]);
		pixelToRun = Utility.decodeURIComponent(pixelToRun);
		if(pixelToRun == null) {
			throw new IllegalArgumentException("Must define the pixel to run in parallel");
		}

		PixelParallelWorker worker = new PixelParallelWorker();
		worker.setInisight(this.insight);
		worker.setPixel(pixelToRun);
		
		ParallelThread pt = new ParallelThread();
		pt.worker = worker;
		java.lang.Thread t = new Thread(pt);
		t.start();
			
		NounMetadata noun = new NounMetadata("Staring pixel job in parallel", PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
		return noun;
	}

}
