package prerna.reactor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class ParallelRunReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ParallelRunReactor.class);

	public ParallelRunReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PARALLEL_WORKER.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String className = keyValue.get(keysToGet[0]);
		if(className == null) {
			throw new SemossPixelException(getError("No worker defined"));
		}

		NounMetadata noun = new NounMetadata("Staring job in parallel", PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
		try {
			Object opw = Class.forName(className).newInstance();
			if(opw == null || !(opw instanceof IParallelWorker)) {
				throw new SemossPixelException(getError("Worker must be IParallelWorker"));
			}
			
			// execute
			IParallelWorker pw = (IParallelWorker) opw;
			pw.setInisight(insight);
			ParallelThread pt = new ParallelThread();
			pt.worker = pw;
			java.lang.Thread t = new Thread(pt);
			t.start();
			
		} catch (InstantiationException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(getError("Cannot Instantiate class " + className));
		} catch (IllegalAccessException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(getError("Illegal Access class " + className));
		} catch (ClassNotFoundException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(getError("Not Found  class " + className));
		}

		return noun;
	}

}
