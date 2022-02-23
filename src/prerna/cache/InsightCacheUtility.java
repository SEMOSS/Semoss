package prerna.cache;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import prerna.auth.utils.SecurityInsightUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.impl.InsightAdministrator;
import prerna.engine.impl.SmssUtilities;
import prerna.om.Insight;
import prerna.project.api.IProject;
import prerna.sablecc2.reactor.cluster.VersionReactor;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.MosfetSyncHelper;
import prerna.util.Utility;
import prerna.util.gson.InsightAdapter;

public class InsightCacheUtility {

	private static final Logger logger = LogManager.getLogger(InsightCacheUtility.class);

	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	private static byte[] buffer = new byte[2048];

	public static final String INSIGHT_ZIP = "InsightZip.zip";
	public static final String MAIN_INSIGHT_JSON = "InsightCache.json";
	public static final String VIEW_JSON = "ViewData.json";
	
	public static final String VERSION_FILE = ".version";
	public static final String VERSION_HEADER = 
			"# This file is automatically generated by SEMOSS.\r\n" + 
			"# It is not intended for manual editing.\r\n";
	public static final String DATETIME_KEY = VersionReactor.DATETIME_KEY;
	public static final String VERSION_KEY = VersionReactor.VERSION_KEY;
	
	public static final String CACHE_FOLDER = ".cache";
	
	private InsightCacheUtility() {
		
	}
	
	public static String getInsightCacheFolderPath(Insight insight, Map<String, Object> parameters) {
		String rdbmsId = insight.getRdbmsId();
		String projectId = insight.getProjectId();
		String projectName = insight.getProjectName();
		return getInsightCacheFolderPath(projectId, projectName, rdbmsId, parameters);
	}
	
	public static String getInsightCacheFolderPath(String projectId, String projectName, String rdbmsId, Map<String, Object> parameters) {
		String folderDir = AssetUtility.getProjectAssetVersionFolder(projectName, projectId) 
				+ DIR_SEPARATOR +  rdbmsId + DIR_SEPARATOR + CACHE_FOLDER;
		return folderDir;
	}
	
	/**
	 * Main method to cache a full insight
	 * @param insight
	 * @throws IOException 
	 */
	public static File cacheInsight(Insight insight, Set<String> varsToExclude, Map<String, Object> parameters) throws IOException {
		String rdbmsId = insight.getRdbmsId();
		String projectId = insight.getProjectId();
		String projectName = insight.getProjectName();
		
		if(projectId == null || rdbmsId == null || projectName == null) {
			throw new IOException("Cannot jsonify an insight that is not saved");
		}
		
		String folderDir = getInsightCacheFolderPath(insight, parameters);
		String normalizedFolderDir = Utility.normalizePath(folderDir);
		if(!(new File(normalizedFolderDir).exists())) {
			new File(normalizedFolderDir).mkdirs();
		}
		File zipFile = new File(normalizedFolderDir + DIR_SEPARATOR + INSIGHT_ZIP);

		FileOutputStream fos = null;
		ZipOutputStream zos = null;
		try {
			fos = new FileOutputStream(zipFile.getAbsolutePath());
			zos = new ZipOutputStream(fos);
			
			InsightAdapter iAdapter = new InsightAdapter(zos);
			iAdapter.setVarsToExclude(varsToExclude);
			StringWriter writer = new StringWriter();
			JsonWriter jWriter = new JsonWriter(writer);
			iAdapter.write(jWriter, insight);
			
			String insightLoc = normalizedFolderDir + DIR_SEPARATOR + MAIN_INSIGHT_JSON;
			File insightFile = new File(insightLoc);
			try {
				FileUtils.writeStringToFile(insightFile, writer.toString());
			} catch (IOException e) {
				logger.error(Constants.STACKTRACE, e);
			}
			addToZipFile(insightFile, zos);

			// also write a .version file so store when this cache was created
			String versionFileLoc = normalizedFolderDir + DIR_SEPARATOR + VERSION_FILE;
			File versionFile = writeInsightCacheVersion(versionFileLoc);
			addToZipFile(versionFile, zos);

			// update the metadata
			IProject project = Utility.getProject(projectId);
			LocalDateTime cachedOn = LocalDateTime.now();
			InsightAdministrator admin = new InsightAdministrator(project.getInsightDatabase());
			admin.updateInsightCachedOn(rdbmsId, cachedOn);
			SecurityInsightUtils.updateInsightCachedOn(projectId, rdbmsId, cachedOn);
			
			String mosfetPath = MosfetSyncHelper.getMosfetFileLocation(projectId, projectName, rdbmsId);
			File mosfet = new File(mosfetPath);
			if(mosfet.exists() && mosfet.isFile()) {
				MosfetSyncHelper.updateMosfitFileCachedOn(mosfet, cachedOn);
			}
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			closeStream(zos);
			closeStream(fos);
		}
		
		return zipFile;
	}
	
	/**
	 * 
	 * @param versionFileLoc
	 * @return
	 */
	public static File writeInsightCacheVersion(String versionFileLoc) {
		StringBuilder version = new StringBuilder(VERSION_HEADER);
		version.append(DATETIME_KEY).append("=").append(LocalDateTime.now()).append("\r\n");
		try {
			Map<String, String> versionMap = VersionReactor.getVersionMap(false);
			version.append(VERSION_KEY).append("=").append(versionMap.get(VERSION_KEY)).append("\r\n");
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
		}
		
		File versionFile = new File(versionFileLoc);
		try {
			FileUtils.writeStringToFile(versionFile, version.toString());
		} catch (IOException e) {
			logger.error(Constants.STACKTRACE, e);
		}
		return versionFile;
	}
	
	/**
	 * Used to add a file to the insight zip
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
			closeStream(fis);
		}
	}
	
	/**
	 * Main method to read in a full insight
	 * @param insightDir
	 * @return
 	 * @throws IOException 
	 */
	public static Insight readInsightCache(File insightCacheZip, Insight existingInsight) throws IOException, RuntimeException {
		ZipFile zip = null;
		ZipEntry entry = null;
		InputStream is = null;
		InputStreamReader isr = null;
		BufferedReader br = null;
		try {
			zip = new ZipFile(insightCacheZip);
			entry = zip.getEntry(MAIN_INSIGHT_JSON);
			if(entry == null) {
				throw new IOException("Invalid zip format for cached insight");
			}
			is = zip.getInputStream(entry);
			isr = new InputStreamReader(is);
			br = new BufferedReader(isr);
	        StringBuilder sb = new StringBuilder();
	        String line;
	        while ((line = br.readLine()) != null) {
	            sb.append(line);
	        }
	        
	        InsightAdapter iAdapter = new InsightAdapter(zip);
	        iAdapter.setUserContext(existingInsight);
	        StringReader reader = new StringReader(sb.toString());
	        JsonReader jReader = new JsonReader(reader);
			Insight insight = iAdapter.read(jReader);
			return insight;
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw e;
		} finally {
			closeStream(br);
			closeStream(isr);
			closeStream(is);
			closeStream(zip);
		}
	}
	
	/**
	 * Main method to read in a full insight
	 * @param insightPath
	 * @return
	 * @throws IOException 
	 */
	public static Insight readInsightCache(String insightPath, Insight existingInsight) throws IOException, JsonSyntaxException {
		File insightFile = new File(insightPath);
		return readInsightCache(insightFile, existingInsight);
	}
	
	/**
	 * Get the view data for a cached insight
	 * @return
	 * @throws IOException 
	 */
	public static Map<String, Object> getCachedInsightViewData(Insight insight, Map<String, Object> parameters) throws IOException, JsonSyntaxException {
		String rdbmsId = insight.getRdbmsId();
		String projectId = insight.getProjectId();
		String projectName = insight.getProjectName();
		
		if(projectId == null || rdbmsId == null || projectName == null) {
			throw new IOException("Cannot jsonify an insight that is not saved");
		}
		
		String zipFileLoc = getInsightCacheFolderPath(insight, parameters) + DIR_SEPARATOR + INSIGHT_ZIP;
		String normalizedZipFileLoc = Utility.normalizePath(zipFileLoc);
		File zipFile = new File(normalizedZipFileLoc);

		if(!zipFile.exists()) {
			throw new IOException("Cannot find insight cache");
		}
		
		ZipEntry viewData = new ZipEntry(VIEW_JSON);
		ZipFile zip = null;
		InputStream zis = null;
		try {
			zip = new ZipFile(normalizedZipFileLoc);
			zis = zip.getInputStream(viewData);
			
			String jsonString = IOUtils.toString(zis, "UTF-8");
			Gson gson = new Gson();
			return gson.fromJson(jsonString, Map.class);
		} catch(Exception e) {
			logger.error("Error retrieving cache for " + rdbmsId);
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(zis != null) {
				zis.close();
			}
			if(zip != null) {
				zip.close();
			}
		}
		return null;
	}
	
	/**
	 * Delete cached files for an insight
	 * @param projectId
	 * @param projectName
	 * @param rdbmsId
	 */
	public static void deleteCache(String projectId, String projectName, String rdbmsId, Map<String, Object> parameters, boolean pullCloud) {
		// this is false on save insight
		// because i do not want to pull when i save
		// but i do want to delete the cache in case i am saving 
		// from an existing insight as the .cache folder gets moved

		String folderDir = getInsightCacheFolderPath(projectId, projectName, rdbmsId, parameters);
		Path appFolder = Paths.get(DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR 
				+ Constants.PROJECT_FOLDER + DIR_SEPARATOR + SmssUtilities.getUniqueName(projectName, projectId));
		Path relative = appFolder.relativize( Paths.get(folderDir));
		if(pullCloud) {
			ClusterUtil.reactorPullProjectFolder(projectId, folderDir, relative.toString());
		}

		File folder = new File(Utility.normalizePath(folderDir)); 
		if(!folder.exists()) {
			return;
		}
		
		File[] cacheFiles = folder.listFiles();
		for(File f : cacheFiles) {
			if(f.isDirectory()) {
				ICache.deleteFolder(f);
			} else {
				ICache.deleteFile(f);
			}
		}
		
		// update the metadata
		try {
			IProject project = Utility.getProject(projectId);
			LocalDateTime cachedOn = null;
			InsightAdministrator admin = new InsightAdministrator(project.getInsightDatabase());
			admin.updateInsightCachedOn(rdbmsId, cachedOn);
			SecurityInsightUtils.updateInsightCachedOn(projectId, rdbmsId, cachedOn);
			
			String mosfetPath = MosfetSyncHelper.getMosfetFileLocation(projectId, projectName, rdbmsId);
			File mosfet = new File(mosfetPath);
			if(mosfet.exists() && mosfet.isFile()) {
				MosfetSyncHelper.updateMosfitFileCachedOn(mosfet, cachedOn);
			}
		} catch (IOException e) {
			logger.error(Constants.STACKTRACE, e);
		}
		
		if(pullCloud) {
			ClusterUtil.reactorPushProjectFolder(projectId, folderDir, relative.toString());
		}
	}
	
	public static void unzipFile(ZipFile zip, String name, String path) throws FileNotFoundException {
		byte[] buffer = new byte[1024];
		File newFile = new File(Utility.normalizePath(path));
		FileOutputStream fos = null;
		ZipEntry zipE = new ZipEntry(name);
		InputStream zis = null;
		try {
			zis = zip.getInputStream(zipE);
			fos = new FileOutputStream(newFile);
			int len;
            while ((len = zis.read(buffer)) > 0) {
                fos.write(buffer, 0, len);
            }
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(fos != null) {
				try {
					fos.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
			if(zis != null) {
				try {
					zis.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}
	
	/**
	 * Close a stream
	 * @param is
	 */
	private static void closeStream(Closeable is) {
		if(is != null) {
			try {
				is.close();
			} catch (IOException e) {
				logger.error(Constants.STACKTRACE, e);
			}
		}
	}
	
}
