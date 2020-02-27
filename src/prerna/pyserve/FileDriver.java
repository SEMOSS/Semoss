package prerna.pyserve;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CyclicBarrier;

import org.apache.commons.io.FileUtils;

import prerna.ds.py.PyUtils;
import prerna.util.DIHelper;

public class FileDriver implements Runnable{

	String folderToWatch =  "c:/users/pkapaleeswaran/workspacej3/temp/filebuffer";
	CyclicBarrier barr = null;
	Map outputs = new Hashtable();
	String internalLock ="Internal Lock";
	
	
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		
		// super simple driver
		// take my command
		// write to a file
		// 
		
		DIHelper helper = DIHelper.getInstance();
		Properties prop = new Properties();
		try {
			prop.load(new FileInputStream("c:/users/pkapaleeswaran/workspacej3/MonolithDev5/RDF_Map_web.prop"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		helper.setCoreProp(prop);

		
		String mainFolder = "c:/users/pkapaleeswaran/workspacej3/temp/filebuffer";
		String newFolder = PyUtils.getInstance().getTempTupleSpace("dummy", mainFolder);
		
		System.out.println("Main Folder.. " + newFolder);
		
		FileDriver fd = new FileDriver();
		Thread thread = new Thread(fd);
		fd.folderToWatch = newFolder;
		//thread.start();
		fd.getNextCommand();
		
		

	}
	
	// this is what run script should be going forward
	public void getNextCommand()
	{
		try {
			int count = 0;
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			String data = null;


			while((data = br.readLine()) != null)
			{
				// write it to a file
				// wait for the thread to inform you
				String fileName = "f"+count + ".py";
				String scriptFile = folderToWatch + "/" + fileName;
				FileUtils.writeStringToFile(new File(scriptFile), data);

				WatchService watchService = FileSystems.getDefault().newWatchService();
				Path path = Paths.get(folderToWatch);
				WatchKey watchKey = path.register(
						  watchService, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY);
				
				WatchKey key;
				Commandeer comm = new Commandeer();
				comm.file = scriptFile;
				Thread commandThread = new Thread(comm);
				commandThread.start();
				//key = watchService.take();
				while ((key = watchService.take()) != null) 
				{
					boolean breakout = false;
				    for (WatchEvent<?> event : key.pollEvents()) {
				    		String file = (String)(event.context().toString());
				    		Kind kind = event.kind();
				    		//System.out.println("Event kind:" + kind + ". File affected: " + file + ".");
				    		//if(kind == StandardWatchEventKinds.ENTRY_CREATE)
							if(kind == StandardWatchEventKinds.OVERFLOW)
								System.err.println("Interesting.. got here " + file);
				    		if(file.endsWith(".completed"))
				    		{
				    			printOutput(file);
				    			breakout = true;
				    			//break;
				    		}
				    		//else if()// this is a modify request 
				    		//	processComplete(file);
							key.reset();
				    		//barr.await();
				    }
				    if(breakout)
				    	break;
				}

				
				System.out.println("Enter next command.. : ");
				count++;
				
				/*while(!outputs.containsKey(fileName))
				{
					synchronized(internalLock)
					{
						internalLock.wait(10);
					}
					System.out.println(".");
				}
				printOutput(fileName);
				*/	
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		

	}
	
	@Override
	public void run() {
		try {
			WatchService watchService = FileSystems.getDefault().newWatchService();
			Path path = Paths.get(folderToWatch);
			WatchKey watchKey = path.register(
					  watchService, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY);
			
			WatchKey key;
			//key = watchService.take();
			// this will go through once and end
			while ((key = watchService.take()) != null) 
			{
				boolean breakout = false;
			    for (WatchEvent<?> event : key.pollEvents()) {
			    		String file = (String)(event.context().toString());
			    		Kind kind = event.kind();
			    		//System.out.println("Event kind:" + kind + ". File affected: " + file + ".");
			    		//if(kind == StandardWatchEventKinds.ENTRY_CREATE)
						key.reset();
						if(kind == StandardWatchEventKinds.OVERFLOW)
							System.err.println("Interesting.. got here " + file);
			    		if(file.endsWith(".completed"))
			    		{
			    			processOutput(file);
			    			//breakout = true;
			    			//break;
			    		}
			    		//else if()// this is a modify request 
			    		//	processComplete(file);
			    		//barr.await();
			    }
			    if(breakout)
			    	break;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.err.println("Out of the run loop");

	}
	
	public void printOutput(String file)
	{
		try {
		file = file.replace(".state.completed", "");
		String command;
		System.err.println("Reading from file.. " + file);
		File outputFile = new File(folderToWatch + "/" + file + ".output");
		if(outputFile.exists())
		{
			command = FileUtils.readFileToString(outputFile).trim();
			System.out.println("Output >>" + command);
		}	
			// create a new file now
		new File(folderToWatch + "/" + file + ".state.delivered").createNewFile();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
	}
	
	
	public void processOutput(String file)
	{
		System.out.println("processing output");
		outputs.put(file.replace(".state.completed", ""),"true");
		synchronized(internalLock)
		{
			internalLock.notify();
		}
		System.out.println("processing output done");
	}
	

}
