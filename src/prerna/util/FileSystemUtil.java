package prerna.util;

import java.io.File;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.io.FilenameUtils;

import prerna.auth.User;

public final class FileSystemUtil {

	/**
	 * 
	 * @param user
	 * @param filePath
	 * @param relativeFilePath
	 * @param pathSubstringIndex
	 * @return
	 */
	public static List<Map<String, Object>> browseFileSystem(User user, String filePath, String relativeFilePath,
			int pathSubstringIndex) {
		File directory = new File(filePath);
		if (!directory.exists()) {
			throw new IllegalArgumentException(
					"The directory " + relativeFilePath + " does not exist within the assets folder");
		}
		if (!directory.isDirectory()) {
			throw new IllegalArgumentException(
					"The path " + relativeFilePath + " exists within the assets folder but is not a directory");
		}

		DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss")
				.withZone(user.getZoneId());

		List<Map<String, Object>> retObj = new ArrayList<>();
		File[] allFiles = directory.listFiles();
		for (File f : allFiles) {
			if (f.getName().startsWith(".") && f.isDirectory()) {
				// we dont want to show this
				continue;
			}
			Map<String, Object> fileMap = new HashMap<>();
			fileMap.put("name", f.getName());
			String path = f.getAbsolutePath().substring(pathSubstringIndex).replace("\\", "/");
			if (f.isDirectory()) {
				fileMap.put("type", "directory");
				path = path + "/";
			} else {
				fileMap.put("type", FilenameUtils.getExtension(f.getName()));
			}
			fileMap.put("path", path);
			fileMap.put("lastModified", dateTimeFormatter.format(Instant.ofEpochMilli(f.lastModified())));
			retObj.add(fileMap);
		}

		return retObj;
	}

	/**
	 * 
	 * @param dir
	 * @param pattern
	 * @param baseLen
	 * @param results
	 * @param dateTimeFormatter
	 */
	public static void searchRecursive(File dir, Pattern pattern, int baseLen, List<Map<String, Object>> results,
			DateTimeFormatter dateTimeFormatter) {
		File[] entries = dir.listFiles();
		if (entries == null) {
			return;
		}

		for (File f : entries) {
			String name = f.getName();
			// skip hidden directory
			if (f.isDirectory() && name.startsWith(".")) {
				continue;
			}
			// build relative path
			String rel = f.getAbsolutePath().substring(baseLen).replace('\\', '/');
			// match
			if (pattern.matcher(name).find()) {
				Map<String, Object> meta = createMeta(f, rel, f.isDirectory(), dateTimeFormatter);
				results.add(meta);
			}
			// recurse
			if (f.isDirectory()) {
				searchRecursive(f, pattern, baseLen, results, dateTimeFormatter);
			}
		}
	}

	/**
	 * 
	 * @param f
	 * @param relativePath
	 * @param isDir
	 * @param dateTimeFormatter
	 * @return
	 */
	private static Map<String, Object> createMeta(File f, String relativePath, boolean isDir,
			DateTimeFormatter dateTimeFormatter) {
		Map<String, Object> map = new HashMap<>();
		map.put("name", f.getName());
		map.put("path", relativePath);
		map.put("lastModified", dateTimeFormatter.format(Instant.ofEpochMilli(f.lastModified())));
		map.put("type", isDir ? "directory" : FilenameUtils.getExtension(f.getName()));
		return map;
	}
}
