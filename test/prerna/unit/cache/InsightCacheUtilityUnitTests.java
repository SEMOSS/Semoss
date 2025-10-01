package prerna.unit.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.auth.utils.SecurityInsightUtils;
import prerna.cache.InsightCacheUtility;
import prerna.om.Insight;
import prerna.project.api.IProject;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class InsightCacheUtilityUnitTests {

	@Test
	public void testGetInsightCacheFolderPath(@TempDir File tempDir) throws Exception {
		// insight folder test vars
		String rdbmsId = "rdbmsId";
		String projectId = "projectId";
		String projectName = "projectName";
		Insight in = new Insight();
		in.setRdbmsId(rdbmsId);
		in.setProjectId(projectId);
		in.setProjectName(projectName);

		// set up base folder
		File baseFolder = new File(tempDir, "baseFolder");
		String fileSeparator = java.nio.file.FileSystems.getDefault().getSeparator();
		try (MockedStatic<DIHelper> mockedSingleton = Mockito.mockStatic(DIHelper.class)) {
			DIHelper instance = Mockito.mock(DIHelper.class);
			when(DIHelper.getInstance()).thenReturn(instance);
			when(instance.getProperty(Constants.BASE_FOLDER)).thenReturn(baseFolder.getAbsolutePath());

			// test method
			String insightFolderPath = InsightCacheUtility.getInsightCacheFolderPath(in, null);

			// validate
			String expectedPath = Utility
					.normalizePath(baseFolder.getAbsolutePath() + fileSeparator + "project" + fileSeparator
							+ projectName + "__" + projectId + fileSeparator)
					+ "app_root" + "/" + "version" + fileSeparator + rdbmsId + fileSeparator + ".cache";
			assertEquals(expectedPath, insightFolderPath);
		}
	}

	@Test
	public void testGetInsightCacheFolderPath2(@TempDir File tempDir) throws Exception {
		// insight folder test vars
		String rdbmsId = "rdbmsId";
		String projectId = "projectId";
		String projectName = "projectName";

		// set up base folder
		File baseFolder = new File(tempDir, "baseFolder");
		String fileSeparator = java.nio.file.FileSystems.getDefault().getSeparator();
		try (MockedStatic<DIHelper> mockedSingleton = Mockito.mockStatic(DIHelper.class)) {
			DIHelper instance = Mockito.mock(DIHelper.class);
			when(DIHelper.getInstance()).thenReturn(instance);
			when(instance.getProperty(Constants.BASE_FOLDER)).thenReturn(baseFolder.getAbsolutePath());

			// test method
			String insightFolderPath = InsightCacheUtility.getInsightCacheFolderPath(projectId, projectName, rdbmsId,
					new HashMap<>());
			// validate
			String expectedPath = Utility
					.normalizePath(baseFolder.getAbsolutePath() + fileSeparator + "project" + fileSeparator
							+ projectName + "__" + projectId + fileSeparator)
					+ "app_root" + "/" + "version" + fileSeparator + rdbmsId + fileSeparator + ".cache";
			assertEquals(expectedPath, insightFolderPath);
		}
	}

	@Test
	public void testGetInsightCacheFolderPathWithParams(@TempDir File tempDir) throws Exception {
		// insight folder test vars
		String rdbmsId = "rdbmsId";
		String projectId = "projectId";
		String projectName = "projectName";
		Map<String, Object> params = new HashMap<>();
		Vector v = new Vector<String>();
		v.add("Yes");
		v.add("No");
		params.put("colX", v);

		// set up base folder
		File baseFolder = new File(tempDir, "baseFolder");
		String fileSeparator = java.nio.file.FileSystems.getDefault().getSeparator();
		try (MockedStatic<DIHelper> mockedSingleton = Mockito.mockStatic(DIHelper.class)) {
			DIHelper instance = Mockito.mock(DIHelper.class);
			when(DIHelper.getInstance()).thenReturn(instance);
			when(instance.getProperty(Constants.BASE_FOLDER)).thenReturn(baseFolder.getAbsolutePath());

			// test method
			String insightFolderPath = InsightCacheUtility.getInsightCacheFolderPath(projectId, projectName, rdbmsId,
					params);

			// validate base path
			String expectedPath = Utility
					.normalizePath(baseFolder.getAbsolutePath() + fileSeparator + "project" + fileSeparator
							+ projectName + "__" + projectId + fileSeparator)
					+ "app_root" + "/" + "version" + fileSeparator + rdbmsId + fileSeparator + ".cache" + fileSeparator;
			assertTrue(insightFolderPath.startsWith(expectedPath));

			// validate params to hex value
//			System.out.println(insightFolderPath);
			int lastSlash = insightFolderPath.lastIndexOf("\\");
			String hex = insightFolderPath.substring(lastSlash + 1);
			assertEquals("a719c9556fea102a118269c9615b5fffdf4ff3f28f9c35789a33928553463c30", hex);
//			System.out.println(hex);
		}
	}

	@Test
	public void testCacheInsight(@TempDir File tempDir) throws Exception {
		// insight folder test vars
		String rdbmsId = "rdbmsId";
		String projectId = "projectId";
		String projectName = "projectName";
		Insight in = new Insight();
		in.setRdbmsId(rdbmsId);
		in.setProjectId(projectId);
		in.setProjectName(projectName);
		in.runPixel("Date();");

		// set up rdfMap to load
		File baseFolder = new File(tempDir, "baseFolder");
		String rdfMapFilePath = tempDir + "RDF_MAP.prop";
		Properties rdfMap = new Properties();
		rdfMap.put(Constants.BASE_FOLDER, baseFolder.getAbsolutePath());
		try (FileOutputStream fileOutputStream = new FileOutputStream(rdfMapFilePath)) {
			// Store properties to file
			rdfMap.store(fileOutputStream, "rdf map properties");
		} catch (IOException e) {
			e.printStackTrace();
		}

		DIHelper.getInstance().loadCoreProp(rdfMapFilePath);

		// test method
		File zipFile = InsightCacheUtility.cacheInsight(in, new HashSet<>(), new HashMap<>());

		System.out.println(zipFile.getAbsolutePath());

		// validate
//			String expectedPath =  Utility.normalizePath(baseFolder.getAbsolutePath() + fileSeparator + "project" + fileSeparator + projectName
//					+ "__" + projectId  + fileSeparator)  + "app_root" + fileSeparator + "version" + fileSeparator+ rdbmsId
//					+ fileSeparator + ".cache";
//			assertEquals(expectedPath, insightFolderPath);

	}

	@Test
	public void testWriteInsightCacheVersion(@TempDir File tempDir) throws Exception {
		File versionFilePath = new File(tempDir, ".version");
		assertFalse(versionFilePath.exists());
		versionFilePath = InsightCacheUtility.writeInsightCacheVersion(versionFilePath.getAbsolutePath());
		assertTrue(versionFilePath.exists());

		String contents = FileUtils.readFileToString(versionFilePath);
		assertTrue(contents.startsWith(InsightCacheUtility.VERSION_HEADER));
	}

	@Test
	public void testAddToZipFile(@TempDir File tempDir) throws Exception {
		// create file to add to zip
		File fileToAdd = new File(tempDir, "hello.txt");
		fileToAdd.createNewFile();
		FileUtils.writeStringToFile(fileToAdd, "hello world", Charset.defaultCharset());

		// create zip file
		String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
		File zipFile = new File(tempDir + DIR_SEPARATOR + "test.zip");

		FileOutputStream fos = new FileOutputStream(zipFile.getAbsolutePath());
		try (ZipOutputStream zos = new ZipOutputStream(fos)) {
			try {
				countFiles(zipFile.getAbsolutePath());
				fail("zip should be empty");
			} catch (Exception e) {
				assertEquals("zip file is empty", e.getMessage());
			}
			InsightCacheUtility.addToZipFile(fileToAdd, zos);
			zos.close();
			assertEquals(1, countFiles(zipFile.getAbsolutePath()));
		}
		fos.close();

	}

	private static int countFiles(String zipFilePath) throws IOException {
		int fileCount = 0;
		try (ZipFile zipFile = new ZipFile(zipFilePath)) {
			Enumeration<? extends ZipEntry> entries = zipFile.entries();
			while (entries.hasMoreElements()) {
				ZipEntry entry = entries.nextElement();
				if (!entry.isDirectory()) {
					fileCount++;
				}
			}
		}
		return fileCount;
	}

	@Test
	public void testReadInsightCache() {

	}

	@Test
	public void testReadInsightCacheFromInsight() {

	}

	@Test
	public void testGetCachedInsightViewData() {

	}

	@Test
	public void testDeleteCache(@TempDir File tempDir) throws Exception {
		String fileSeparator = java.nio.file.FileSystems.getDefault().getSeparator();
		String baseFolderPath = "baseFolder";
		File baseFolder = new File(tempDir, baseFolderPath);
		baseFolder.mkdir();
		File baseProjFolder = new File(tempDir, baseFolderPath + fileSeparator + "project");
		baseProjFolder.mkdir();

		// insight folder test vars
		String rdbmsId = "rdbmsId";
		String projectId = "projectId";
		String projectName = "projectName";
		String myProjSmss = projectName + "__" + projectId + ".smss";
		File projSmss = new File(baseProjFolder, myProjSmss);

		Properties projProps = new Properties();
		projProps.setProperty(Constants.PROJECT, projectId);
		projProps.setProperty(Constants.PROJECT_ALIAS, projectName);
		projProps.setProperty(Constants.PROJECT_TYPE, "prerna.project.impl.Project");
		projProps.setProperty(Constants.RDBMS_INSIGHTS, "project/@PROJECT@/insights_database");
		projProps.setProperty(Constants.RDBMS_INSIGHTS_TYPE, "H2_DB");
		projProps.setProperty(Constants.DRIVER, "org.h2.Driver");
		projProps.setProperty(Constants.RDBMS_TYPE, "H2_DB");
		projProps.setProperty(Constants.USERNAME, "sa");
		projProps.setProperty(Constants.PASSWORD, "");
		projProps.setProperty(Constants.CONNECTION_URL, "jdbc:h2:nio:@BaseFolder@/project/" + projectName + "__"
				+ projectId
				+ "/insights_database;query_timeout=180000;early_filter=true;query_cache_size=24;cache_size=32768");

		try (FileOutputStream out = new FileOutputStream(projSmss)) {
			projProps.store(out, "Project Properties");
		} catch (IOException e) {
			e.printStackTrace();
		}

		String myProjectFolderPath = Utility.normalizePath(baseFolder.getAbsolutePath() + fileSeparator + "project"
				+ fileSeparator + projectName + "__" + projectId);

		Insight in = new Insight();
		in.setRdbmsId(rdbmsId);
		in.setProjectId(projectId);
		in.setProjectName(projectName);

		// set up base folder
		try (MockedStatic<DIHelper> mockedSingleton = Mockito.mockStatic(DIHelper.class)) {
			DIHelper instance = Mockito.mock(DIHelper.class);
			MockedStatic<SecurityInsightUtils> securityInsightUtilsMockedStatic = Mockito
					.mockStatic(SecurityInsightUtils.class);
			when(DIHelper.getInstance()).thenReturn(instance);
			when(instance.getProperty(Constants.BASE_FOLDER)).thenReturn(baseFolder.getAbsolutePath());
			when(instance.getProjectProperty(projectId + "_" + Constants.STORE)).thenReturn(projSmss.getAbsolutePath());

			// insight folder is empty
			String insightCacheFolderPath = InsightCacheUtility.getInsightCacheFolderPath(projectId, projectName,
					rdbmsId, null);
			// need to make sure the insight cache folder exists
			File projectFolder = new File(myProjectFolderPath);
			assertTrue(projectFolder.exists());

			File versionFolder = new File(myProjectFolderPath + fileSeparator + "app_root" + fileSeparator + "version");
			assertTrue(versionFolder.exists());

			File rdbmsFolder = new File(myProjectFolderPath + fileSeparator + "app_root" + fileSeparator + "version"
					+ fileSeparator + rdbmsId);
			rdbmsFolder.mkdir();
			assertTrue(rdbmsFolder.exists());

			File cacheFolder = new File(myProjectFolderPath + fileSeparator + "app_root" + fileSeparator + "version"
					+ fileSeparator + rdbmsId + fileSeparator + ".cache");
			cacheFolder.mkdir();
			assertTrue(cacheFolder.exists());
			assertEquals(0, cacheFolder.list().length);

			// add files to cache folder to test deletion
			File testFolder = new File(insightCacheFolderPath + fileSeparator + "testFolder");
			testFolder.mkdir();
			assertTrue(testFolder.exists());

			File testFile = new File(insightCacheFolderPath + fileSeparator + "hello.txt");
			testFile.createNewFile();
			assertTrue(testFile.exists());

			assertEquals(2, cacheFolder.list().length);

			// test method
			InsightCacheUtility.deleteCache(projectId, projectName, rdbmsId, null, false);
			assertEquals(0, cacheFolder.list().length);

			IProject proj = Utility.getProject(projectId);
			proj.close();
		}
	}

	@Test
	public void testUnzipFile(@TempDir File tempDir) throws Exception {

		// create file to add to zip
		String fileContents = "hello world!!!!!!!!!";
		File fileToAdd = new File(tempDir, "hello.txt");
		fileToAdd.createNewFile();
		FileUtils.writeStringToFile(fileToAdd, fileContents, Charset.defaultCharset());

		// create zip file
		String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
		File zipFile = new File(tempDir + DIR_SEPARATOR + "test.zip");

		FileOutputStream fos = new FileOutputStream(zipFile.getAbsolutePath());
		try (ZipOutputStream zos = new ZipOutputStream(fos)) {
			InsightCacheUtility.addToZipFile(fileToAdd, zos);
			zos.close();
			assertEquals(1, countFiles(zipFile.getAbsolutePath()));
		}
		fos.close();
		// remove temp file added to zip
		fileToAdd.delete();
		assertFalse(fileToAdd.exists());

		// unzip file
		ZipFile zip = new ZipFile(zipFile);
		String newFile = tempDir.getAbsolutePath() + "\\output.txt";
		InsightCacheUtility.unzipFile(zip, "hello.txt", newFile);
		zip.close();

		// validations
		File unzippedFile = new File(newFile);
		assertTrue(unzippedFile.exists());
	}

}
