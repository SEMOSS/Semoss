package prerna.util.git;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.plexus.util.FileUtils;

import prerna.util.Constants;
import prerna.util.Utility;


public class GitAssetUtils {

	private static final Logger classLogger = LogManager.getLogger(GitAssetUtils.class);

	// gets all the information on assets
	// gives the information in 2 chunks
	// files and directories
	public static Map<String, List<String>> browse(String gitFolder, String replacer) {
		return browse(gitFolder, replacer, "");
	}
	
	
	public static String getDate(long time)
	{
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
		return dateFormat.format(time);
	}
	

	/**
	 * Get all the assets with metadata
	 * 
	 * @param gitFolder
	 * @param replacer
	 * @return
	 */
	public static List<Map<String, Object>> getAssetMetadata(String gitFolder, String replacer) {
		List<Map<String, Object>> retList = new Vector<>();
		File folder = new File(gitFolder);
		File[] listOfFiles = folder.listFiles();
		String repString = ""; // can be $IF

		for (File f : listOfFiles) {
			Map<String, Object> fileMap = new HashMap<>();
			fileMap.put("name", f.getName());
			fileMap.put("lastModified", getDate(f.lastModified()));
			String relative = new File(replacer).toURI().relativize(new File(f.getAbsolutePath()).toURI()).getPath();
			fileMap.put("path", relative);
			if (f.isFile()) {
				String path = f.getAbsolutePath().replaceAll("\\\\", "/");
				if (replacer != null) {
					path = path.replaceAll(replacer, repString);
				}
				// System.out.println("File " + path);
				path = path.replaceFirst("/", "");
				fileMap.put("type", FilenameUtils.getExtension(path));
//				fileMap.put("size", f.length());
			} else if (f.isDirectory()) {
				String path = f.getName().replaceAll("\\\\", "/");
				// no hidden files
				if (!path.startsWith(".")) {
					if (replacer != null) {
						path = path.replaceAll(replacer, repString);
					}
				}
				fileMap.put("type", "directory");
			}
			retList.add(fileMap);
		}

		return retList;
	}

	/**
	 * Get all the assets with metadata
	 * 
	 * @param gitFolder
	 * @param replacer
	 * @return
	 */
	public static List<Map<String, Object>> getAssetMetadata(String gitFolder, String replacer, String prefix, boolean addApp) {
		List<Map<String, Object>> retList = new Vector<>();
		File folder = new File(gitFolder);
		File[] listOfFiles = folder.listFiles();
		if(listOfFiles == null) {
			return retList;
		}
		String repString = ""; // can be $IF

		for (File f : listOfFiles) {
			Map<String, Object> fileMap = new HashMap<>();
			fileMap.put("name", f.getName());
			fileMap.put("lastModified", getDate(f.lastModified()));
			String relative = new File(replacer).toURI().relativize(new File(f.getAbsolutePath()).toURI()).getPath();
			relative = prefix + relative;
			fileMap.put("path", relative);
			if (f.isFile()) {
				String path = f.getAbsolutePath().replaceAll("\\\\", "/");
				if (replacer != null) {
					path = path.replaceAll(replacer, repString);
				}
				// System.out.println("File " + path);
				path = path.replaceFirst("/", "");
				fileMap.put("type", FilenameUtils.getExtension(path));
//				fileMap.put("size", f.length());
			} else if (f.isDirectory()) {
				String path = f.getName().replaceAll("\\\\", "/");
				// no hidden files
				if (!path.startsWith(".")) {
					if (replacer != null) {
						path = path.replaceAll(replacer, repString);
					}
				}
				fileMap.put("type", "directory");
			}
			retList.add(fileMap);
		}
		if(addApp)
		{
			Map <String, Object> appFolder = new HashMap<>();
			
			appFolder.put("name", "app_assets");
			appFolder.put("lastModified", "");
			appFolder.put("path", "app_assets/");
			appFolder.put("type",  "directory");
			retList.add(appFolder);
		}

		return retList;
	}

	
	
	// recursively print it
	public void getAssets(String gitFolder, List <String> dirList) {
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
				if(!path.startsWith(".")) {
					dirList.add(listOfFiles[i].getAbsolutePath());
				}
			}
		}
		
		if(!dirList.isEmpty()) {
			getAssets(dirList.remove(0), dirList);
		}
	}

	// should this also commit ?
	public static void deleteAsset(String  assetFolder) {
		try {
			File assetFile = new File(assetFolder);
			if(assetFile.exists()) {
				if(assetFile.isDirectory()) {
					FileUtils.deleteDirectory(assetFile);
				} else {
					assetFile.delete();
				}
			}
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}

	// this is to get all the assets
	// with a particular extension
	// runs through recursion to get what is needed
	public static List<String> listAssets(String gitFolder, String extn, String replacer, List<String> dirList, List<String> retList) {
		if(dirList == null) {
			dirList = new ArrayList<String>();
		}
		if(retList == null) {
			retList = new ArrayList<String>();
		}

		File folder = new File(gitFolder);
		File[] listOfFiles = folder.listFiles();

		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].isFile()) {
				System.out.println("File " + listOfFiles[i].getAbsolutePath());
				String path = listOfFiles[i].getAbsolutePath();
				path = path.replaceAll("\\\\", "/");
				if(isMatch(Utility.getInstanceName(path),extn)) {
					if(replacer != null) {
						path = path.replaceAll(replacer, "");
					}
					path.replaceFirst("/", "");
					retList.add(path);
				}
			} else if (listOfFiles[i].isDirectory()) {
				//System.out.println("Directory " + listOfFiles[i].getName());
				String path = listOfFiles[i].getName();
				if(!path.startsWith(".")) // no hidden files
					dirList.add(listOfFiles[i].getAbsolutePath());
			}
		}

		if(!dirList.isEmpty()) {
			return listAssets(dirList.remove(0), extn, replacer, dirList, retList);
		} 

		return retList;
	}
	
	public static List<Map<String, Object>> listAssetMetadata(String gitFolder, String extn, String replacer, List<String> dirList, List<Map<String, Object>> retList) {
		if (dirList == null) {
			dirList = new ArrayList<String>();
		}
		if (retList == null) {
			retList = new ArrayList<>();
		}

		File folder = new File(gitFolder);
		File[] listOfFiles = folder.listFiles();

		for (File f : listOfFiles) {
			Map<String, Object> fileMap = new HashMap<>();
			fileMap.put("name", f.getName());
			String relative = new File(replacer).toURI().relativize(new File(f.getAbsolutePath()).toURI()).getPath();
			fileMap.put("path", relative);
			fileMap.put("lastModified", getDate(f.lastModified()));
			if (f.isFile()) {
				String path = f.getAbsolutePath();
				path = path.replaceAll("\\\\", "/");
				if (isMatch(Utility.getInstanceName(path), extn)) {
					if (replacer != null) {
						path = path.replaceAll(replacer, "");
					}

					path.replaceFirst("/", "");
					fileMap.put("type", FilenameUtils.getExtension(path));
					retList.add(fileMap);
				}
			} else if (f.isDirectory()) {
				String path = f.getName();
				if (!path.startsWith(".")) { // no hidden files
					dirList.add(f.getAbsolutePath());
					if (isMatch(path, extn)) {
						fileMap.put("path", path);
						fileMap.put("type", "directory");
						retList.add(fileMap);
					}
				}
			}
		}

		if (!dirList.isEmpty()) {
			return listAssetMetadata(dirList.remove(0), extn, replacer, dirList, retList);
		}

		return retList;
	}
	
	public static boolean isMatch(String s, String p) {
		int i = 0;
		int j = 0;
		int starIndex = -1;
		int iIndex = -1;
	 
		while (i < s.length()) {
			if (j < p.length() && (p.charAt(j) == '?' || p.charAt(j) == s.charAt(i))) {
				++i;
				++j;
			} else if (j < p.length() && p.charAt(j) == '*') {
				starIndex = j;		
				iIndex = i;
				j++;
			} else if (starIndex != -1) {
				j = starIndex + 1;
				i = iIndex+1;
				iIndex++;
			} else {
				return false;
			}
		}
	 
		while (j < p.length() && p.charAt(j) == '*') {
			++j;
		}
	 
		return j == p.length();
	}
	
	// gets all the information on assets
	// gives the information in 2 chunks
	// files and directories
	public static Map<String, List<String>> browse(String gitFolder, String replacer, String replaceWith) {
		Map<String, List<String>> retHash = new Hashtable<String, List<String>>();

		List<String> files = new ArrayList<String>();
		List<String> directories = new ArrayList<String>();
		List<String> fileDates = new ArrayList<String>();
		List<String> dirDates = new ArrayList<String>();

		File folder = new File(gitFolder);
		File[] listOfFiles = folder.listFiles();
		String repString = replaceWith; // can be $IF

		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].isFile()) {
				//String path = listOfFiles[i].getAbsolutePath().replaceAll("\\\\", "/");
				String path = listOfFiles[i].getName();
				// we probably dont need this anymore
				/*
				if(replacer != null) {
					path = path.replaceAll(replacer, repString);
				}*/
				files.add(path.replaceFirst("/", ""));
				String time = getDate(listOfFiles[i].lastModified());
				fileDates.add(time);
			} else if (listOfFiles[i].isDirectory()) {
				// System.out.println("Directory " + listOfFiles[i].getName());
				String path = listOfFiles[i].getName().replaceAll("\\\\", "/");
				// no hidden files
				if(!path.startsWith(".")) {
					if(replacer != null) {
						path = path.replaceAll(replacer, repString);
					}
					directories.add(path);
				}
				String time = getDate(listOfFiles[i].lastModified());
				dirDates.add(time);
			}
		}
		retHash.put("FILE_LIST", files);
		retHash.put("DIR_LIST", directories);
		retHash.put("FILE_DATE", fileDates);
		retHash.put("DIR_DATE", dirDates);
		
		return retHash;
	}

	
}
