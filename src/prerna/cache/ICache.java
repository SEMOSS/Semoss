package prerna.cache;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.UnsupportedEncodingException;

import org.apache.commons.io.FileUtils;
import org.apache.tika.io.IOUtils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public interface ICache {
	
	String FILE_SEPARATOR = System.getProperty("file.separator");
	
	static String cleanFolderAndFileName(String s){
		return s.replaceAll("[^a-zA-Z0-9-_\\.]", "_");
	}
	
	static void writeToFile(String fileName, Object vec) {
		FileOutputStream os = null;
		try {
			Gson gson = new GsonBuilder().disableHtmlEscaping().serializeSpecialFloatingPointValues().setPrettyPrinting().create();
			String data = gson.toJson(vec);
			os = new FileOutputStream(new File(fileName));
			IOUtils.write(data, os);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if(os != null) {
					os.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	static String readFromFileString(String fileName) {
    	Reader is = null;
    	FileReader fr = null;
        try {
        	File file = new File(fileName);
        	if(file.exists() && file.isFile()) {
	        	fr = new FileReader(new File(fileName));
	            is = new BufferedReader(fr);
	            String retData = IOUtils.toString(is);
	            return retData;
        	}
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
        	try {
	        	if(fr != null) {
	        		fr.close();
	        	}
	        	if(is != null) {
					is.close();
	        	}
        	} catch (IOException e) {
				e.printStackTrace();
			}
        }

        return null;
	}
	
	static void deleteFolder(String folderLocation) {
		File basefolder = new File(folderLocation);
		if(basefolder.isDirectory()) {
			try {
				FileUtils.forceDelete(basefolder);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	static void deleteFile(File file) {
		if(file.isFile()) {
			try {
				FileUtils.forceDelete(file);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	static void deleteFile(String fileLocation) {
		File file = new File(fileLocation);
		deleteFile(file);
	}
	
}
