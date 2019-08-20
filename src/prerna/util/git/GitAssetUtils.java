package prerna.util.git;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.codehaus.plexus.util.FileUtils;

import prerna.util.Utility;

public class GitAssetUtils {

	// gets all the information on assets
	// gives the information in 2 chunks
	// files and directories
	public static Map<String, List<String>> browse(String gitFolder, String replacer) {
		Map<String, List<String>> retHash = new Hashtable<String, List<String>>();

		List<String> files = new ArrayList<String>();
		List<String> directories = new ArrayList<String>();
		List<String> fileDates = new ArrayList<String>();
		List<String> dirDates = new ArrayList<String>();

		File folder = new File(gitFolder);
		File[] listOfFiles = folder.listFiles();
		String repString = ""; // can be $IF

		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].isFile()) {
				//String path = listOfFiles[i].getAbsolutePath().replaceAll("\\\\", "/");
				String path = listOfFiles[i].getName();
				// we probably dont need this anymore
				/*
				if(replacer != null) {
					path = path.replaceAll(replacer, repString);
				}*/
				System.out.println("File " + path);
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
	
	
	private static String getDate(long time)
	{
		SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
		return dateFormat.format(time);
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
			e.printStackTrace();
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
	
}
