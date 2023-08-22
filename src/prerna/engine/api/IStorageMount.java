package prerna.engine.api;

import java.util.List;
import java.util.Map;

public interface IStorageMount {

	// set any of the properties needed to open this engine
	public void setProperty(String key, String value);
	
	// opens the storage
	public void openLocation();
	
	// browse a directory
	// first array 
	/*
	 * 		retHash.put("FILE_LIST", files);
		retHash.put("DIR_LIST", directories);
		retHash.put("FILE_DATE", fileDates);
		retHash.put("DIR_DATE", dirDates);
	 */
	// format [{path=classes/compileerror.out, name=compileerror.out, lastModified=05/08/2020 13:27:09, type=out}, {path=classes/prerna/, name=prerna, lastModified=05/08/2020 12:32:08, type=directory}]
	public List <Map<String, Object>>  browse(String startDir);
	
	
	// add a file
	// if the newname is null then this will use the same as the local file name
	public void addFile(String localFileName, String location, String newname);
	
	// delete a particular file
	public void deleteLocation(String location);
	
	// replaces the file
	public void replaceFile(String localFileName, String storedFile);
	
	// create a new file
	// this may not work for things like amazon S3 where the folder is just the name
	public void createLocation(String baseLocation, String nameOfNewFile);
	
	// copy file to a given location
	// remote location to local location
	public void copyFile(String remlocation, String localLocation);
	
}
