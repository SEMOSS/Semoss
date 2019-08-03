package prerna.util.git;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import org.codehaus.plexus.util.FileUtils;

public class GitAssetUtils {

	// gets all the information on assets
	// gives the information in 2 chunks
	// files and directories
	public static Hashtable browse(String gitFolder)
	{
		Hashtable retHash = new Hashtable();
		// list or string ?
		
		List files = new ArrayList<String>();
		List directories = new ArrayList<String>();
		retHash.put("FILE_LIST", "none");
		retHash.put("DIR_LIST", "none");
		
		File folder = new File(gitFolder);
		File[] listOfFiles = folder.listFiles();

		for (int i = 0; i < listOfFiles.length; i++) {
		  if (listOfFiles[i].isFile()) {
		    System.out.println("File " + listOfFiles[i].getAbsolutePath());
	    	files.add(listOfFiles[i].getAbsolutePath());
		  } else if (listOfFiles[i].isDirectory()) {
		    //System.out.println("Directory " + listOfFiles[i].getName());
			  String path = listOfFiles[i].getName();
			  if(!path.startsWith(".")) // no hidden files
				  directories.add(listOfFiles[i].getAbsolutePath());
		  }
		}

		if(directories.size() > 0)
			retHash.put("DIR_LIST", directories.toString());
		if(files.size() > 0)
			retHash.put("FILE_LIST", directories.toString());
		
		
		return retHash;
		
	}

	// recursively print it
	public void getAssets(String gitFolder, List <String> dirList)
	{
		File folder = new File(gitFolder);
		File[] listOfFiles = folder.listFiles();
		if(dirList == null)
			dirList = new ArrayList<String>();

		for (int i = 0; i < listOfFiles.length; i++) {
		  if (listOfFiles[i].isFile()) {
		    System.out.println("File " + listOfFiles[i].getAbsolutePath());
		  } else if (listOfFiles[i].isDirectory()) {
		    //System.out.println("Directory " + listOfFiles[i].getName());
			  String path = listOfFiles[i].getName();
			  if(!path.startsWith("."))
				  dirList.add(listOfFiles[i].getAbsolutePath());
		  }
		}
		if(dirList.size() == 0)
			return;
		else
			getAssets(dirList.remove(0), dirList);
		
	}
	
	// should this also commit ?
	public static void deleteAsset(String  assetFolder)
	{
		try {
			File assetFile = new File(assetFolder);
			if(assetFile.exists())
			{
				if(assetFile.isDirectory())
					FileUtils.deleteDirectory(assetFile);
				else
					assetFile.delete();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	// this is to get all the assets
	// with a particular extension
	// runs through recursion to get what is needed
	public static List listAssets(String gitFolder, String extn, ArrayList <String> dirList, ArrayList <String> retList)
	{
		Hashtable retHash = new Hashtable();
		StringBuilder files = new StringBuilder();
		StringBuilder directories = new StringBuilder();
		retHash.put("FILE_LIST", "none");
		retHash.put("DIR_LIST", "none");
		
		if(dirList == null)
			dirList = new ArrayList<String>();
		
		if(retList == null)
			retList = new ArrayList<String>();
		
		File folder = new File(gitFolder);
		File[] listOfFiles = folder.listFiles();

		for (int i = 0; i < listOfFiles.length; i++) {
		  if (listOfFiles[i].isFile()) {
		    System.out.println("File " + listOfFiles[i].getAbsolutePath());
		    String path = listOfFiles[i].getAbsolutePath();
		    if(path.endsWith(extn))
		    	retList.add(listOfFiles[i].getAbsolutePath());
		  } else if (listOfFiles[i].isDirectory()) {
		    //System.out.println("Directory " + listOfFiles[i].getName());
			  String path = listOfFiles[i].getName();
			  if(!path.startsWith(".")) // no hidden files
				  dirList.add(listOfFiles[i].getAbsolutePath());
		  }
		}

		if(dirList.size() > 0)
			return listAssets(dirList.remove(0),extn, dirList, retList);
		else 
			return retList;
		
	}



	
}
