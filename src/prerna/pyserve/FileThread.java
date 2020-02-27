package prerna.pyserve;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.python.google.common.io.Files;

public class FileThread implements Runnable {
	
	String folderToWatch = null;
	String extnToWatch = "py.go";
	IWorker worker = null;
	private static final String CLASS_NAME = FileThread.class.getName();

	public static final Logger LOGGER = LogManager.getLogger(CLASS_NAME);

	public boolean keepAlive = true;

	// initiated, working, complete / failed, relayed, cached
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		// TODO Auto-generated method stub
		LOGGER.info("Starting watch on.. " + folderToWatch);
		try {
			
			// create the ready file
			File readFile = new File(folderToWatch + "/ready");
			readFile.createNewFile();
			
			
			WatchService watchService = FileSystems.getDefault().newWatchService();
			Path path = Paths.get(folderToWatch);
			WatchKey watchKey = path.register(
					  watchService, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY);
			
			WatchKey key;
			while ((key = watchService.take()) != null && keepAlive) {
			    for (WatchEvent<?> event : key.pollEvents()) {
			    		String file = (String)(event.context().toString());
			    		Kind kind = event.kind();
			    		//System.out.println("Event kind:" + kind + ". File affected: " + file + ".");
			    		//if(kind == StandardWatchEventKinds.ENTRY_CREATE)
			    		try
			    		{
				    		if(file.endsWith(extnToWatch))
				    			worker.processCommand(folderToWatch, file);
				    		else if(file.endsWith(".delivered"))
				    			worker.processCleanup(folderToWatch, file);
				    		else if(file.endsWith(".admin"))
				    			worker.processAdmin(folderToWatch, file);
				    		else if(file.endsWith(".closeall")) // kill this thread there is no need to watch anymore
				    		{
				    			worker.processCleanup(folderToWatch);
				    			break;
				    		}
				    		else if(file.endsWith(".force_close"))
				    		{
				    			keepAlive = false;
				    			break;
				    		}
				    		//else if()// this is a modify request 
				    		//else if(!file.endsWith(".completed"))
				    		key.reset();
			    		}catch(Exception ex)
			    		{
			    			ex.printStackTrace();
			    		}
			    		//	processComplete(file);
			    }
			    if(!keepAlive)
			    	break;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		LOGGER.info("Folder " + folderToWatch + " Thread Ended");
	}
	


	// process the file	

	public void setWorker(IWorker worker)
	{
		this.worker = worker;
	}
	
	
	

}
