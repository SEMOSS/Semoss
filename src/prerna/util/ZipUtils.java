package prerna.util;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
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

import com.google.gson.Gson;

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
		File dir = new File(folderPath);
		addAllToZip(dir, zos, null);
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
			String subPrefix = file.getName();
			if (prefix != null) {
				subPrefix = prefix + FILE_SEPARATOR + file.getName();
			}
			File[] files = file.listFiles();
			for (File subF : files) {
				addAllToZip(subF, zos, subPrefix);
			}
		} else {
			String fileName = file.getName();
			if (prefix != null) {
				fileName = prefix + FILE_SEPARATOR + file.getName();
			}
			ZipEntry zipEntry = new ZipEntry(fileName);
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
	 * 
	 * @param zipFilePath
	 * @param destDirectory
	 * @return Map of list of files depending on if it is a DIR or FILE
	 * @throws IOException
	 */
	public static Map<String, List<String>> unzip(String zipFilePath, String destination) throws IOException {
		// grab list of files that are being unzipped
		Map<String, List<String>> files = listFilesInZip(Paths.get(zipFilePath));
		// unzip files
		ZipFile zipIn = new ZipFile(zipFilePath);
		Enumeration<? extends ZipEntry> entries = zipIn.entries();
		while (entries.hasMoreElements()) {
			ZipEntry entry = entries.nextElement();
			String filePath = destination + FILE_SEPARATOR + entry.getName();
			if (entry.isDirectory()) {
				File file = new File(filePath);
				file.mkdirs();
			} else {
				File parent = new File(filePath).getParentFile();
				if (!parent.exists()) {
					parent.mkdirs();
				}
				InputStream is = zipIn.getInputStream(entry);
				extractFile(is, filePath);
				is.close();
			}
		}
		zipIn.close();
		return files;

	}

	/**
	 * Copy file to path
	 * 
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
	
	/**
	 * https://stackoverflow.com/questions/15667125/read-content-from-files-which-are-inside-zip-file
	 * NOTE ::: Cleaning up paths to remove initial / to push files to git
	 * 
	 * @param fromZip
	 * @throws IOException
	 */
	public static Map<String, List<String>> listFilesInZip(Path fromZip) throws IOException {
		FileSystem zipFs = FileSystems.newFileSystem(fromZip, null);
		Map<String, List<String>> paths = new HashMap<>();
		Vector<String> dirs = new Vector<>();
		Vector<String> files = new Vector<>();
		for (Path root : zipFs.getRootDirectories()) {
			Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
					// clean file path for git
					String filePath = file.toString();
					if(file.startsWith("/")) {
						filePath = filePath.toString().replaceFirst("/", "");
						if(!filePath.equals(""))
							files.add(filePath.toString());
					}
					return FileVisitResult.CONTINUE;
				}

				@Override
				public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
					// clean file path for git
					String pathDir = dir.toString().replaceFirst("/", "");
					if(!pathDir.equals(""))
					dirs.add(pathDir.toString());
					return super.preVisitDirectory(dir, attrs);
				}
			});
		}
		zipFs.close();
		paths.put("DIR", dirs);
		paths.put("FILE", files);
		return paths;
	}

	public static void main(String[] args) throws FileNotFoundException, IOException {
		String dest = "C:\\Users\\SEMOSS\\workspace\\Semoss\\db\\Movie__6e41aba8-29da-4616-b2f9-647a8ef01313\\version";
		dest = "C:\\Users\\SEMOSS\\workspace";
		dest = dest.replace("\\", "/");
		String zip = "C:\\Users\\SEMOSS\\Downloads\\Movie.zip";
		zip = zip.replace("\\", "/");
		Path zipUri = Paths.get(zip);		
		Map<String, List<String>> map = listFilesInZip(zipUri);
		Gson gson = GsonUtility.getDefaultGson();
		System.out.println(gson.toJson(map));

	}

}
