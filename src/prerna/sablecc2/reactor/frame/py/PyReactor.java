package prerna.sablecc2.reactor.frame.py;

import java.util.List;
import java.util.Vector;

import org.apache.log4j.Logger;

import prerna.ds.py.PyExecutorThread;
import prerna.ds.py.PyUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Utility;

public class PyReactor extends AbstractReactor {
	
	private static final String CLASS_NAME = PyReactor.class.getName();
	
	@Override
	public NounMetadata execute() {
		if(!PyUtils.pyEnabled()) {
			throw new IllegalArgumentException("Python is not enabled to use the following command");
		}
		
		Logger logger = getLogger(CLASS_NAME);
		PyExecutorThread pyThread = this.insight.getPy();
		Object lock = pyThread.getMonitor();

		String code = Utility.decodeURIComponent(this.curRow.get(0).toString());
		logger.info("Execution python script: " + code);
		pyThread.command = new String[]{code};
		
		Object output = "";
		synchronized(lock) {
			try {
				lock.notify();
				lock.wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			// waking up now
			output = pyThread.response.get(code);
		}
		
		List<NounMetadata> outputs = new Vector<NounMetadata>(1);
		outputs.add(new NounMetadata(output, PixelDataType.CONST_STRING));
		return new NounMetadata(outputs, PixelDataType.CODE, PixelOperationType.CODE_EXECUTION);
	}

}
