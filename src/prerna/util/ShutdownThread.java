package prerna.util;

import prerna.engine.impl.r.RSingleton;

public class ShutdownThread implements Runnable{

	static
	{
		Runtime r=Runtime.getRuntime(); 
		r.addShutdownHook((new Thread(new ShutdownThread())));  
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		// couple of things I need to do
		
		// close out all the R connections that are open
		// this is basically the ones in the RSingleton
		// secondly I need to remove all the h2 files I generated
		// last I need to remove all the temp folders
		
		System.out.println("SEMOSS Exiting.... performing routines");
		
		// need to check if this is RServe and then shutdown
		if(DIHelper.getInstance().getProperty(Constants.R_CONNECTION_JRI).equalsIgnoreCase("false"))
			RSingleton.stopRServe();
				
	}

}
