package prerna.pyserve;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.plexus.util.FileUtils;

public class CleanerThread extends Thread{
	
	
	// takes command and executes it
	// quite simple
	public String folder = null;
	
	public static final Logger LOGGER = LogManager.getLogger(CleanerThread.class.getName());
	
	public CleanerThread(String folder)
	{
		this.folder = folder;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		int attempt = 1;
		boolean deleted = false;
		while(attempt < 10 && !deleted)
		{
			try {
				FileUtils.deleteDirectory(folder);
				deleted = true;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				attempt++;
				try {
					Thread.sleep(attempt * 1000);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
		}		
		
		if(attempt >= 10)
			LOGGER.info("Unable to delete Directory " + folder);
		else
			LOGGER.info("Deleted directory " + folder);
	}

}
