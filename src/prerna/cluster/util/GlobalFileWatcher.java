package prerna.cluster.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;

import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import prerna.util.Constants;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class GlobalFileWatcher implements CuratorCacheListener {

	protected static final Logger logger = LogManager.getLogger(GlobalFileWatcher.class);

	String semossHome = null;
	DBSynchronizer dbs = null;
	
	public GlobalFileWatcher(String semossHome, DBSynchronizer dbs)
	{
		this.semossHome = semossHome;
		this.dbs = dbs;
	}
	
	
	public void processNodeChanged(String path, String payload)
	{
		System.out.println("Processing for " + path);
		String folder = semossHome + "/db" + path;
		
		String lastModified = getLastModifiedTime(folder);
		System.err.println("Payload is " + payload + " <<>> " + lastModified);
		File file = new File(folder);
		String parent = file.getParent();
		System.err.println("Parent Folder " + parent);
		
		// possibly the information is not there
		if(lastModified != null && lastModified.contentEquals(payload))
		{
			System.out.println("All in sync");
		}
		else
		{
			// run minio
			System.out.println("Going to run minio to pull something");
			// make a object lock for this path
			Object pathMonitor = new Object();
			
			
			
			// get to the parent file 
			// synchronize that ?
			if(path.endsWith("mosfet"))
			{
				// pull the whole directory
				// given we capture only specific files
				
				// for this track the directory
				dbs.pathThreadLock.put(parent, pathMonitor);
				// so here we have to get this path monitor and then do what we need to do

				
			}
			else
			{
				// pull the specific file
				dbs.pathThreadLock.put(folder, pathMonitor);
				
			}
			
			// after you are done with everything else
			// remove it
			dbs.pathThreadLock.remove(folder);
			
			// if it ends with .db
			// then stop the engine
			// do whatever else
			
			// logic for writing an insight
			// get the date modified of the current .mosfet file - old_last_modified
			// acquire the interprocess lock 
			// once you acquire - this means that you have the write permission on the zk
			// see if the dbsynchronizer has a path monitor for this path if not null then next..
			// if null nothing to worry.. go at it
			// synchronize the pathMonitor lock - this is important because some other process may be updating this particular directory or file some place else for instance here
			// once you get that - pick the lastModified on the file - new_last_modified
			// check if the old_last_modified == new_last_modified
			// if so.. you are all set proceed
			// else - throw an error back and see if the user would like to save it as something else
		}
	}


	public void processNodeDeleted(String path)
	{
		// get the current version
		// compare it with what is in the database
		// if they are different initiate a minio sync
		// I wonder if we can just delete it directly here instead of a minio sync
		System.out.println("Processing for " + path);
		String folder = semossHome + "/	db" + path;
		System.err.println("Delete path from local.. " + folder);
	}
	

	@Override
	public void event(Type type, ChildData oldData, ChildData data) {
		// TODO Auto-generated method stub
		System.out.println("Type >> " +  type);
		String path = data.getPath();
		String payload = new String(data.getData());
		if(type == Type.NODE_CHANGED || type == Type.NODE_CREATED)
		{
			// do what you need to do
			processNodeChanged(data.getPath(), payload);
		}
		else if(type == Type.NODE_DELETED)
		{
			// do what you need to do
			System.out.println("Remove path from minio " + path);
			processNodeDeleted(path);
		}
	}


	private String getLastModifiedTime(String path)
	{
		try {
			BasicFileAttributes attr = Files.readAttributes(Paths.get(path), BasicFileAttributes.class);
			FileTime ft = attr.lastAccessTime();
			
			return ft.toString();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error(Constants.STACKTRACE, e);
		}
		
		return null;
	}
}
