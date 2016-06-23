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
	
	/**
	 * Removes invalid characters in a folder/file name and replaces with an underscore
	 * @param s				The input folder/file name
	 * @return				A clean version of the input folder/file name
	 */
	static String cleanFolderAndFileName(String s){
		return s.replaceAll("[^a-zA-Z0-9-_\\.]", "_");
	}
	
	/**
	 * Writes an object to the specified file path
	 * The object is converted into a JSON object and then written to the file as a string
	 * @param filePath		The file location		
	 * @param vec			The objec tot write to the file
	 */
	static void writeToFile(String filePath, Object vec) {
		FileOutputStream os = null;
		try {
			// convert the POJO into a JSON string
			Gson gson = new GsonBuilder().disableHtmlEscaping().serializeSpecialFloatingPointValues().setPrettyPrinting().create();
			String data = gson.toJson(vec);
			os = new FileOutputStream(new File(filePath));
			// write the JSON string into the file
			IOUtils.write(data, os);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			// close the streams
			try {
				if(os != null) {
					os.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Ingest a file as a string
	 * @param filePath		The file path to ingest
	 * @return				The string containing the contents of the file
	 */
	static String readFromFileString(String filePath) {
    	Reader is = null;
    	FileReader fr = null;
        try {
        	File file = new File(filePath);
        	if(file.exists() && file.isFile()) {
	        	fr = new FileReader(new File(filePath));
	            is = new BufferedReader(fr);
	            // ingest all the file contents into a string
	            String retData = IOUtils.toString(is);
	            return retData;
        	}
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
        	// close the readers
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
	
	/**
	 * Deletes a folder and all its sub-directories
	 * @param folderLocation	The location of the folder
	 */
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
	
	/**
	 * Deletes a file
	 * @param file				The File object to delete
	 */
	static void deleteFile(File file) {
		if(file.isFile()) {
			try {
				FileUtils.forceDelete(file);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Deletes a file
	 * @param fileLocation		The path to the file 
	 */
	static void deleteFile(String fileLocation) {
		File file = new File(fileLocation);
		deleteFile(file);
	}
	
}
