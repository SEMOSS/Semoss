package prerna.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Properties;

import javax.swing.JDesktopPane;
import javax.swing.JInternalFrame;
import javax.swing.JProgressBar;

import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;

public abstract class AbstractFileWatcher implements Runnable, FilenameFilter{
	
	// opens up a thread and watches the file
	// when available, it will upload it into the journal
	// may be this is a good time to put this on tomcat


	Logger logger = Logger.getLogger(getClass());
	protected JInternalFrame progressBarFrame;
	
	// processes the files with the given extension
	
	protected String folderToWatch = null;
	protected String extension = null;
	protected IEngine engine = null;
	Object monitor = null;
	
	
	public void setFolderToWatch(String folderToWatch)
	{
		this.folderToWatch = folderToWatch;
	}

	public void setExtension(String extension)
	{
		this.extension = extension;
	}
	
	public void setEngine(IEngine engine)
	{
		this.engine = engine;
	}
	
	public void setMonitor(Object monitor)
	{
		this.monitor = monitor;
	}
	
	public abstract void loadFirst();
	
	@Override
	public void run() 
	{
		try
		{
			WatchService watcher = FileSystems.getDefault().newWatchService();
			Path dir2Watch = Paths.get(folderToWatch);

			WatchKey key = dir2Watch.register(watcher, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY);
			while(true)
			{
				//WatchKey key2 = watcher.poll(1, TimeUnit.MINUTES);
				WatchKey key2 = watcher.take();
				
				for(WatchEvent<?> event: key2.pollEvents())
				{
					WatchEvent.Kind kind = event.kind();
					if(kind == StandardWatchEventKinds.ENTRY_CREATE)
					{
						String newFile = event.context() + "";
						if(newFile.endsWith(extension))
						{
							Thread.sleep(2000);	
							try
							{
								process(newFile);
								
							}catch(Exception ex)
							{
								ex.printStackTrace();
							}
						}else
							logger.info("Ignoring File " + newFile);
					}
				}
				key2.reset();
			}
		}catch(Exception ex)
		{
			// do nothing - I will be working it in the process block
		}
	}	

	@Override
	public boolean accept(File arg0, String arg1) 
	{
		// TODO Auto-generated method stub
		return arg1.endsWith(extension);
	}
	
	public abstract void process(String fileName);	
	
	public void showProgressBar(String text){
		JDesktopPane pane = (JDesktopPane) DIHelper.getInstance().getLocalProp(Constants.DESKTOP_PANE);
		JProgressBar bar = new JProgressBar();
		bar.setIndeterminate(true);
		bar.setStringPainted(true);
		bar.setString(text);
		progressBarFrame = new JInternalFrame();
		progressBarFrame.getContentPane().add(bar);
		pane.add(progressBarFrame);
		progressBarFrame.pack();
		progressBarFrame.setVisible(true);
	}
}
