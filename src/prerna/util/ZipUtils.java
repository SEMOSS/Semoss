package prerna.util;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.FileUtils;

import prerna.util.gson.GsonUtility;

public final class ZipUtils {

	// buffer for read and write data to file
	private static byte[] buffer = new byte[2048];

	public static final String FILE_SEPARATOR = System.getProperty("file.separator");

	private ZipUtils() {

	}

	/**
	 * Zip files within a dir
	 * 
	 * @param folderPath
	 * @param zipFilePath
	 * @return
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static ZipOutputStream zipFolder(String folderPath, String zipFilePath)
			throws FileNotFoundException, IOException {
		FileOutputStream fos = new FileOutputStream(zipFilePath);
		ZipOutputStream zos = new ZipOutputStream(fos);
		// add every file
		File dir = new File(folderPath);
		File[] files = dir.listFiles();
		if (files != null) {
			for (File file : files) {
				String prefix = file.getParent().substring(file.getParent().lastIndexOf(FILE_SEPARATOR) + 1);
				addAllToZip(file, zos, prefix);
			}
		}
		return zos;
	}

	/**
	 * Add file to ZipOutputStream
	 * 
	 * @param file
	 * @param zos
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static void addToZipFile(File file, ZipOutputStream zos) throws FileNotFoundException, IOException {
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
			if (fis != null) {
				fis.close();
			}
		}
		zos.closeEntry();
	}

	private static void addAllToZip(File file, ZipOutputStream zos, String prefix)
			throws FileNotFoundException, IOException {
		if (file.isDirectory()) {
			String subPrefix = prefix + FILE_SEPARATOR + file.getName();
			File[] files = file.listFiles();
			for (File subF : files) {
				addAllToZip(subF, zos, subPrefix);
			}
		} else {
			ZipEntry zipEntry = new ZipEntry(prefix + FILE_SEPARATOR + file.getName());
			zos.putNextEntry(zipEntry);

			FileInputStream fis = null;
			try {
				int length;
				fis = new FileInputStream(file);
				while ((length = fis.read(buffer)) >= 0) {
					zos.write(buffer, 0, length);
				}
			} finally {
				if (fis != null) {
					fis.close();
				}
			}
			zos.closeEntry();
		}
	}

	private static void unZipEngine(String destinationFolder, String zipFile) {
		File directory = new File(destinationFolder);

		// if the output directory doesn't exist, create it
		if (!directory.exists()) {
			directory.mkdirs();
		}

		String tempAbsolutePath = null;
		FileInputStream fInput = null;
		ZipInputStream zipInput = null;
		try {
			fInput = new FileInputStream(zipFile);
			zipInput = new ZipInputStream(fInput);

			ZipEntry entry = zipInput.getNextEntry();
			while (entry != null) {
				String entryName = entry.getName();
				System.out.println(entryName);
				File file = null;
				if (entryName.endsWith(".smss")) {
					tempAbsolutePath = destinationFolder + FILE_SEPARATOR + entryName.replace(".smss", ".temp");
					file = new File(tempAbsolutePath);
				} else {
					file = new File(destinationFolder + FILE_SEPARATOR + entryName);
				}
				System.out.println("Unzip file " + entryName + " to " + file.getAbsolutePath());
				// create the directory for the next entry
				if (entryName.contains(FILE_SEPARATOR)) {
					String[] dirStructure = entryName.split(FILE_SEPARATOR);
					String absolutePath = destinationFolder + FILE_SEPARATOR + dirStructure[0];
					for (int i = 0; i < dirStructure.length; i++) {
						File newDir = new File(absolutePath);
						if (!newDir.exists()) {
							boolean success = newDir.mkdirs();
							if (success == false) {
								System.out.println("Problem creating Folder");
							}
						}
						// absolutePath = absolutePath + FILE_SEPARATOR +
						// dirStructure[i+1];
					}
				}
				if (!entryName.endsWith(FILE_SEPARATOR)) {
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
				if (zipInput != null) {
					zipInput.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {
				if (fInput != null) {
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

	/**
	 * Unzip files to a folder and track files that have been added
	 * @param zipFilePath
	 * @param destDirectory
	 * @return Map of list of files depending on if it is a DIR or FILE
	 * @throws IOException
	 */
	public static Map<String, List<String>> unzip(String zipFilePath, String destination) throws IOException {
		ZipFile zipIn = new ZipFile(zipFilePath);
		Enumeration<? extends ZipEntry> entries = zipIn.entries();
		Vector<String> dirList = new Vector<String>();
		Vector<String> fileList = new Vector<String>();
		while (entries.hasMoreElements()) {
			ZipEntry entry = entries.nextElement();
			String filePath = destination + entry.getName();
			if (entry.isDirectory()) {
				File file = new File(filePath);
				file.mkdirs();
				dirList.add(entry.getName());
			} else {
				InputStream is = zipIn.getInputStream(entry);
				extractFile(is, filePath);
				is.close();
				fileList.add(entry.getName());
			}
		}
		zipIn.close();
		Map<String, List<String>> addedFiles = new HashMap<>();
		addedFiles.put("DIR", dirList);
		addedFiles.put("FILE", fileList);
		return addedFiles;

	}

	/**
	 * Copy file to path
	 * @param zipIn
	 * @param filePath
	 * @throws IOException
	 */
	private static void extractFile(InputStream zipIn, String filePath) throws IOException {
		BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(filePath));
		byte[] bytesIn = buffer;
		int read = 0;
		while ((read = zipIn.read(bytesIn)) != -1) {
			bos.write(bytesIn, 0, read);
		}
		bos.close();
	}

	public static void main(String[] args) throws FileNotFoundException, IOException {
		String path = "C:\\Users\\rramirezjimenez\\Documents\\workspace\\Semoss\\db\\newMov__a2127aa7-e953-435f-a3ab-53f97028c795\\version\\d4375e9a-6d9d-4954-9385-e93d44e8aa63";
		String destination = "C:\\Users\\rramirezjimenez\\Desktop\\test.zip";
		ZipOutputStream zos = ZipUtils.zipFolder(path, destination);
		zos.close();
	}

}
