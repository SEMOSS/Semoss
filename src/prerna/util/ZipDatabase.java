package prerna.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.FileUtils;

import prerna.engine.impl.SmssUtilities;

public final class ZipDatabase {

	// buffer for read and write data to file
	private static byte[] buffer = new byte[2048];
	
	private static final String FILE_SEPARATOR = "/";//System.getProperty("file.separator");
	
	private static final String OUTPUT_PATH = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/export/ZIPs";
	
	private ZipDatabase() {

	}

	public static void main(String[] args) {
		String path = "C:\\Development\\ZipDB\\AR_Quarterly.zip";
		String destination = "C:\\Development\\ZipDB";
		ZipDatabase.unZipEngine(destination, path);
	}

	private static void unZipEngine(String destinationFolder, String zipFile) {
		File directory = new File(destinationFolder);

		// if the output directory doesn't exist, create it
		if(!directory.exists()) {
			directory.mkdirs();
		}

		String tempAbsolutePath = null;
		FileInputStream fInput = null;
		ZipInputStream zipInput = null;
		try {
			fInput = new FileInputStream(zipFile);
			zipInput = new ZipInputStream(fInput);

			ZipEntry entry = zipInput.getNextEntry();
			while(entry != null){
				String entryName = entry.getName();
				System.out.println(entryName);
				File file = null;
				if(entryName.endsWith(".smss")) {
					tempAbsolutePath = destinationFolder + FILE_SEPARATOR + entryName.replace(".smss", ".temp");
					file = new File(tempAbsolutePath);
				} else {
					file = new File(destinationFolder + FILE_SEPARATOR + entryName);
				}
				System.out.println("Unzip file " + entryName + " to " + file.getAbsolutePath());
				// create the directory for the next entry
				if(entryName.contains(FILE_SEPARATOR)) {
					String[] dirStructure = entryName.split(FILE_SEPARATOR);
					String absolutePath = destinationFolder + FILE_SEPARATOR + dirStructure[0];
					for(int i = 0; i < dirStructure.length; i++) {
						File newDir = new File(absolutePath);
						if(!newDir.exists()) {
							boolean success = newDir.mkdirs();
							if(success == false) {
								System.out.println("Problem creating Folder");
							}
						}
//						absolutePath = absolutePath + FILE_SEPARATOR + dirStructure[i+1];
					}
				}
				if(!entryName.endsWith(FILE_SEPARATOR)){
					// load the file
					FileOutputStream fOutput = new FileOutputStream(file);
					writeFromZipFile(fOutput, zipInput);
				}

				// close ZipEntry and take the next one
				zipInput.closeEntry();
				entry = zipInput.getNextEntry();
			}
			// close the last ZipEntry
			zipInput.closeEntry();

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if(zipInput != null) {
					zipInput.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {
				if(fInput != null) {
					fInput.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		// convert .temp file to .smss
		File tempFile = new File(tempAbsolutePath);
		File smssFile = new File(tempAbsolutePath.replace(".temp", ".smss"));
		try {
			FileUtils.copyFile(tempFile, smssFile);
			smssFile.setReadable(true);
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			FileUtils.forceDelete(tempFile);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static void writeFromZipFile(FileOutputStream fOutput, ZipInputStream zipInput) {
		int count = 0;
		try {
			while ((count = zipInput.read(buffer)) > 0) {
				// write 'count' bytes to the file output stream
				fOutput.write(buffer, 0, count);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				fOutput.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static File zipEngine(String engineId, String engineName) 
	{
		String engineDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/db/" + SmssUtilities.getUniqueName(engineName, engineId);
		String zipFilePath = OUTPUT_PATH + "/" + engineName + ".zip";
		
		FileOutputStream fos = null;
		ZipOutputStream zos = null;
		try {
			fos = new FileOutputStream(zipFilePath);
			zos = new ZipOutputStream(fos);

			// add every file in engine dir folder
			File dir = new File(engineDir);
			File[] files = dir.listFiles();
			if(files != null) {
				for(File file : files) {
					addAllToZip(file, zos);
				}
			}

			// add smss file
			File smss = new File(engineDir + "/../" + SmssUtilities.getUniqueName(engineName, engineId) + ".smss");
			System.out.println("Saving file " + smss.getName());
			addToZipFile(smss, zos);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if(zos != null) {
					zos.flush();
					zos.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {
				if(fos != null) {
					fos.flush();
					fos.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		return new File(zipFilePath);
	}

	private static void addAllToZip(File file, ZipOutputStream zos) throws FileNotFoundException, IOException {
		if(file.isDirectory()) {
			File[] files = file.listFiles();
			for(File f : files) {
				addAllToZip(f, zos);
			}
		} else {
			ZipEntry zipEntry = new ZipEntry(file.getParent().substring(file.getParent().lastIndexOf("\\") + 1) + FILE_SEPARATOR + file.getName());
			zos.putNextEntry(zipEntry);
	
			FileInputStream fis = null;
			try {
				int length;
				fis = new FileInputStream(file);
				while ((length = fis.read(buffer)) >= 0) {
					zos.write(buffer, 0, length);
				}
			} finally {
				if(fis != null) {
					fis.close();
				}
			}
			zos.closeEntry();
		}
	}

	private static void addToZipFile(File file, ZipOutputStream zos) throws FileNotFoundException, IOException {
		ZipEntry zipEntry = new ZipEntry(file.getName());
		zos.putNextEntry(zipEntry);

		FileInputStream fis = null;
		try {
			int length;
			fis = new FileInputStream(file);
			while ((length = fis.read(buffer)) >= 0) {
				zos.write(buffer, 0, length);
			}
		} finally {
			if(fis != null) {
				fis.close();
			}
		}
		zos.closeEntry();
	}

}
