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

public final class ZipDatabase {

	// buffer for read and write data to file
	private static byte[] buffer = new byte[2048];
	
	private ZipDatabase() {

	}

	public static void main(String[] args) {
		String path = "C:\\Users\\mahkhalil\\Desktop\\TAP_Core_Data.zip";
		String destination = "C:\\Users\\mahkhalil\\Desktop";
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
				File file = null;
				if(entryName.endsWith(".smss")) {
					tempAbsolutePath = destinationFolder + File.separator + entryName.replace(".smss", ".temp");
					file = new File(tempAbsolutePath);
				} else {
					file = new File(destinationFolder + File.separator + entryName);
				}
				System.out.println("Unzip file " + entryName + " to " + file.getAbsolutePath());
				// create the directory for the next entry
				if(entryName.contains("\\")) {
					String[] dirStructure = entryName.split("\\\\");
					String absolutePath = destinationFolder + File.separator + dirStructure[0];
					for(int i = 0; i < dirStructure.length - 1; i++) {
						File newDir = new File(absolutePath);
						if(!newDir.exists()) {
							boolean success = newDir.mkdirs();
							if(success == false) {
								System.out.println("Problem creating Folder");
							}
						}
						absolutePath = absolutePath + File.separator + dirStructure[i+1];
					}
				}
				// load the file
				FileOutputStream fOutput = new FileOutputStream(file);
				writeFromZipFile(fOutput, zipInput);

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

	public static void zipEngine(String engineDir, String smssFile, String outputPath) 
	{
		FileOutputStream fos = null;
		ZipOutputStream zos = null;
		try {
			fos = new FileOutputStream(outputPath);
			zos = new ZipOutputStream(fos);

			// add every file in engine dir folder
			File dir = new File(engineDir);
			File[] files = dir.listFiles();
			if(files != null) {
				for(File file : files) {
					System.out.println("Saving file " + file.getName());
					addFolderToZipFile(file, zos);
				}
			}

			// add smss file
			File smss = new File(smssFile);
			System.out.println("Saving file " + smss.getName());
			addToZipFile(smss, zos);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if(zos != null) {
					zos.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {
				if(fos != null) {
					fos.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private static void addFolderToZipFile(File file, ZipOutputStream zos) throws FileNotFoundException, IOException {
		FileInputStream fis = new FileInputStream(file);
		ZipEntry zipEntry = new ZipEntry(file.getParent().substring(file.getParent().lastIndexOf("\\") + 1) + File.separator + file.getName());
		zos.putNextEntry(zipEntry);

		int length;
		while ((length = fis.read(buffer)) >= 0) {
			zos.write(buffer, 0, length);
		}

		zos.closeEntry();
		fis.close();
	}

	private static void addToZipFile(File file, ZipOutputStream zos) throws FileNotFoundException, IOException {
		FileInputStream fis = new FileInputStream(file);
		ZipEntry zipEntry = new ZipEntry(file.getName());
		zos.putNextEntry(zipEntry);

		int length;
		while ((length = fis.read(buffer)) >= 0) {
			zos.write(buffer, 0, length);
		}

		zos.closeEntry();
		fis.close();
	}

}
