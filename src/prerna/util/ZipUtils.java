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
import java.util.zip.ZipOutputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.util.gson.GsonUtility;

public final class ZipUtils {

	private static final Logger classLogger = LogManager.getLogger(ZipUtils.class);

	// buffer for read and write data to file
	private static byte[] buffer = new byte[2048];

	// always need to use this for zipping up and unzipping 
	// traversing will break if the file separator is a "\" 
	// which is generated on windows 
	public static final String FILE_SEPARATOR = "/";

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
	public static ZipOutputStream zipFolder(String folderPath, String zipFilePath) throws FileNotFoundException, IOException {
		return zipFolder(folderPath, zipFilePath, null, null);
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
	public static ZipOutputStream zipFolder(String folderPath, String zipFilePath, List<String> ignoreDirs, List<String> ignoreFiles) throws FileNotFoundException, IOException {
		FileOutputStream fos = new FileOutputStream(zipFilePath);
		ZipOutputStream zos = new ZipOutputStream(fos);
		File dir = new File(folderPath);
		addAllToZip(dir, zos, null, ignoreDirs, ignoreFiles);
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
	
	/**
	 * 
	 * @param logger
	 * @param zos
	 * @param prefixForZip
	 * @param filePathToWrite
	 * @param objToWrite
	 * @throws IOException
	 */
	public static void zipObjectToFile(ZipOutputStream zos, String prefixForZip, String filePathToWrite, Object objToWrite) throws IOException {
		File newFile = new File(filePathToWrite);
		GsonUtility.writeObjectToJsonFile(newFile, new GsonBuilder().setPrettyPrinting().create(), objToWrite);
		ZipUtils.addToZipFile(newFile, zos, prefixForZip);
	}

	/**
	 * Add file to ZipOutputStream
	 * 
	 * @param file
	 * @param zos
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static void addToZipFile(File file, ZipOutputStream zos, String prefix) throws FileNotFoundException, IOException {
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
	
	/**
	 * 
	 * @param file
	 * @param zos
	 * @param prefix
	 * @param ignoreFiles
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private static void addAllToZip(File file, ZipOutputStream zos, String prefix, List<String> ignoreDirs, List<String> ignoreFiles)
			throws FileNotFoundException, IOException {
		if (file.isDirectory()) {
			String subPrefix = file.getName();
			if (prefix != null) {
				subPrefix = prefix + FILE_SEPARATOR + file.getName();
			}
			// make sure its not in the ignore list of folders
			if(ignoreDirs == null || !ignoreDirs.contains(subPrefix)) {
				File[] files = file.listFiles();
				for (File subF : files) {
					addAllToZip(subF, zos, subPrefix, ignoreDirs, ignoreFiles);
				}
			}
		} else {
			String fileName = file.getName();
			if (prefix != null) {
				fileName = prefix + FILE_SEPARATOR + file.getName();
			}
			// make sure its not in the ignore list if we have one
			if(ignoreFiles == null || !ignoreFiles.contains(fileName)) {
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
		ZipFile zipIn = null;
		try {
			zipIn = new ZipFile(Utility.normalizePath(zipFilePath));
			Enumeration<? extends ZipEntry> entries = zipIn.entries();
			while (entries.hasMoreElements()) {
				ZipEntry entry = entries.nextElement();
				String filePath = destination + FILE_SEPARATOR + Utility.normalizePath(entry.getName());
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
		} finally {
			if(zipIn != null) {
				zipIn.close();
			}
		}

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
		BufferedOutputStream bos = null;
		try {
			bos = new BufferedOutputStream(new FileOutputStream(Utility.normalizePath(filePath)));
			byte[] bytesIn = buffer;
			int read = 0;
			while ((read = zipIn.read(bytesIn)) != -1) {
				bos.write(bytesIn, 0, read);
			}
		} finally {
			try{
				if(bos!=null) {
					bos.close();
				}
			}catch(IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
	}

	/**
	 * https://stackoverflow.com/questions/15667125/read-content-from-files-which-are-inside-zip-file
	 * NOTE ::: Cleaning up paths to remove initial / to push files to git
	 * 
	 * @param fromZip
	 * @throws IOException
	 */
	public static Map<String, List<String>> listFilesInZip(Path fromZip) throws IOException {
		FileSystem zipFs = null;
		Map<String, List<String>> paths = new HashMap<>();
		Vector<String> dirs = new Vector<>();
		Vector<String> files = new Vector<>();
		try {
			zipFs = FileSystems.newFileSystem(fromZip, null);
			for (Path root : zipFs.getRootDirectories()) {
				Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
					@Override
					public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
						// clean file path for git
						String filePath = file.toString();
						if(file.startsWith("/")) {
							filePath = filePath.replaceFirst("/", "");
							if(!filePath.equals(""))
								files.add(filePath);
						}
						return FileVisitResult.CONTINUE;
					}

					@Override
					public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
						// clean file path for git
						String pathDir = dir.toString().replaceFirst("/", "");
						if(!pathDir.equals("")) {
							dirs.add(pathDir);
						}

						return super.preVisitDirectory(dir, attrs);
					}
				});
			}
		} finally {
			try{
				if(zipFs!=null) {
					zipFs.close();
				}
			}catch(IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
		paths.put("DIR", dirs);
		paths.put("FILE", files);
		return paths;
	}

//	public static void main(String[] args) throws FileNotFoundException, IOException {
//		//		String dest = "C:\\Users\\SEMOSS\\workspace\\Semoss\\db\\Movie__6e41aba8-29da-4616-b2f9-647a8ef01313\\version";
//		//		dest = "C:\\Users\\SEMOSS\\workspace";
//		//		dest = dest.replace("\\", "/");
//		String zip = "C:\\Users\\mahkhalil\\Desktop\\Movie.zip";
//		zip = zip.replace("\\", "/");
//		Path zipUri = Paths.get(zip);		
//		Map<String, List<String>> map = listFilesInZip(zipUri);
//		Gson gson = GsonUtility.getDefaultGson();
//		classLogger.info(gson.toJson(map));
//	}

}
