package prerna.sablecc2.reactor.frame.py;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class PyInterpreter extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		// TODO Auto-generated method stub
		// Get the current thread and run the input
		
		String command = getCurRow().get(0) + "";
		
		
		PyExecutorThread pyThread = getJep();
		Thread myThread = Thread.currentThread();
		
		Object lock = pyThread.getMonitor();
		pyThread.command = new String[]{command};
		
		Object output = "";
		
		synchronized(lock)
		{
			try {
				lock.notify();
				lock.wait();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			// waking up now
			output = pyThread.response.get(command);
			
			System.out.println("Output >>> " + output);
		}
		
		
		return new NounMetadata(output+"", PixelDataType.CONST_STRING);
	}

}
