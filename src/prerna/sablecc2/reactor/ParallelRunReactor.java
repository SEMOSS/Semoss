package prerna.sablecc2.reactor;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ParallelRunReactor extends AbstractReactor{

	public ParallelRunReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PARALLEL_WORKER.getKey()};
	}

	
	@Override
	public NounMetadata execute() {
		// TODO Auto-generated method stub
		organizeKeys();
		String className = keyValue.get(keysToGet[0]);
		NounMetadata noun = new NounMetadata("Parallel Runner", PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
		if(className == null)
		{
			noun.addAdditionalReturn(getError("Worker not set"));
			return noun;
		}
		try {
			Object opw = Class.forName(className).newInstance();
			if(opw != null)
			{
				if(!(opw instanceof IParallelWorker))
					noun.addAdditionalReturn(getError(className + " Not instance of IParallelWorker"));
				
				IParallelWorker pw = (IParallelWorker)opw;
				pw.setInisight(insight);
				ParallelThread pt = new ParallelThread();
				pt.worker = pw;
				java.lang.Thread t = new Thread(pt);
				t.start();
				
				
			}
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			noun.addAdditionalReturn(getError("Cannot Instantiate class " + className));
			return noun;
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			noun.addAdditionalReturn(getError("Illegal Access class " + className));
			return noun;
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			noun.addAdditionalReturn(getError("Not Found  class " + className));
			return noun;
		}
		
		
		return null;
	}

}
