package prerna.om;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.FileUtils;

import com.google.gson.JsonSyntaxException;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import prerna.engine.impl.SmssUtilities;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.gson.InsightAdapter;

public class InsightCacheUtility {

	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	private static byte[] buffer = new byte[2048];

	public static final String INSIGHT_ZIP = "InsightZip.zip";
	public static final String MAIN_INSIGHT_JSON = "InsightCache.json";
	
	private InsightCacheUtility() {
		
	}
	
	/**
	 * Main method to cache a full insight
	 * @param insight
	 * @throws IOException 
	 */
	public static File cacheInsight(Insight insight) throws IOException {
		String rdbmsId = insight.getRdbmsId();
		String engineId = insight.getEngineId();
		String engineName = insight.getEngineName();
		
		if(engineId == null || rdbmsId == null || engineName == null) {
			throw new IOException("Cannot jsonify an insight that is not saved");
		}
		
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String folderDir = baseFolder + DIR_SEPARATOR + "db" + DIR_SEPARATOR + SmssUtilities.getUniqueName(engineName, engineId) 
				+ DIR_SEPARATOR + "version" + DIR_SEPARATOR + rdbmsId;
		if(!(new File(folderDir).exists())) {
			new File(folderDir).mkdirs();
		}
		File zipFile = new File(folderDir + DIR_SEPARATOR + INSIGHT_ZIP);

		FileOutputStream fos = null;
		ZipOutputStream zos = null;
		try {
			fos = new FileOutputStream(zipFile.getAbsolutePath());
			zos = new ZipOutputStream(fos);
			
			InsightAdapter iAdapter = new InsightAdapter(zos);
			StringWriter writer = new StringWriter();
			JsonWriter jWriter = new JsonWriter(writer);
			iAdapter.write(jWriter, insight);
			
			String insightLoc = folderDir+ DIR_SEPARATOR + MAIN_INSIGHT_JSON;
			File insightFile = new File(insightLoc);
			try {
				FileUtils.writeStringToFile(insightFile, writer.toString());
			} catch (IOException e) {
				e.printStackTrace();
			}
			addToZipFile(insightFile, zos);
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			if(zos != null) {
				zos.close();
			}
			if(fos != null) {
				fos.close();
			}
		}
		
		return zipFile;
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
			if(fis != null) {
				fis.close();
			}
		}
		zos.closeEntry();
	}
	
	/**
	 * Main method to read in a full insight
	 * @param insightDir
	 * @return
	 */
	public static Insight readInsightCache(File insightFile) {
		try {
			ZipFile zip = new ZipFile(insightFile);
			ZipEntry entry = zip.getEntry(MAIN_INSIGHT_JSON);
			InputStream is = zip.getInputStream(entry);
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
	        StringBuilder sb = new StringBuilder();
	        String line;
	        while ((line = br.readLine()) != null) {
	            sb.append(line);
	        }
	        
	        InsightAdapter iAdapter = new InsightAdapter(zip);
	        StringReader reader = new StringReader(sb.toString());
	        JsonReader jReader = new JsonReader(reader);
			Insight insight = iAdapter.read(jReader);
			return insight;
		} catch (JsonSyntaxException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
	/**
	 * Main method to read in a full insight
	 * @param insightPath
	 * @return
	 */
	public static Insight readInsightCache(String insightPath) {
		File insightFile = new File(insightPath);
		return readInsightCache(insightFile);
	}
	
}
