package prerna.pyserve;

import java.io.File;

public class Commandeer implements Runnable {
	
	
	// takes command and executes it
	// quite simple
	String command = null;
	public String file = null;
	int sleepTime = 100; // milliseconds

	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
			Thread.sleep(sleepTime);
			File thisFile = new File(file);
			if(thisFile.exists())
				thisFile.renameTo(new File(file+".go"));
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
