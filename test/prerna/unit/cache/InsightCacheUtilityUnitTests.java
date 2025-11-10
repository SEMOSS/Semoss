package prerna.unit.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.auth.utils.AbstractSecurityUtilsUnitTests;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.cache.InsightCacheUtility;
import prerna.om.Insight;
import prerna.project.api.IProject;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class InsightCacheUtilityUnitTests extends AbstractSecurityUtilsUnitTests {

	@BeforeEach
	void setUp() throws IOException {
		if (Files.exists(projectDir)) {
			FileUtils.cleanDirectory(projectDir.toFile());
		}
	}

	@Test
	public void testGetInsightCacheFolderPath() throws Exception {
		// insight folder test vars
		String rdbmsId = "rdbmsId";
		String projectId = "projectId";
		String projectName = "projectName";
		Insight in = new Insight();
		in.setRdbmsId(rdbmsId);
		in.setProjectId(projectId);
		in.setProjectName(projectName);

		// set up base folder
		String fileSeparator = java.nio.file.FileSystems.getDefault().getSeparator();

		// test method
		String insightFolderPath = InsightCacheUtility.getInsightCacheFolderPath(in, null);

		// validate
		String expectedPath = Utility
				.normalizePath(projectDir + fileSeparator
						+ projectName + "__" + projectId + fileSeparator)
				+ "app_root" + "/" + "version" + fileSeparator + rdbmsId + fileSeparator + ".cache";
		assertEquals(expectedPath, insightFolderPath);
	}

	@Test
	public void testGetInsightCacheFolderPath2() throws Exception {
		// insight folder test vars
		String rdbmsId = "rdbmsId";
		String projectId = "projectId";
		String projectName = "projectName";

		// test method
		String insightFolderPath = InsightCacheUtility.getInsightCacheFolderPath(projectId, projectName, rdbmsId,
				new HashMap<>());
		// validate
		String fileSeparator = java.nio.file.FileSystems.getDefault().getSeparator();
		String expectedPath = Utility
				.normalizePath(projectDir + fileSeparator
						+ projectName + "__" + projectId + fileSeparator)
				+ "app_root" + "/" + "version" + fileSeparator + rdbmsId + fileSeparator + ".cache";
		assertEquals(expectedPath, insightFolderPath);
	}

	@Test
	public void testGetInsightCacheFolderPathWithParams() throws Exception {
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
		String fileSeparator = java.nio.file.FileSystems.getDefault().getSeparator();

		// test method
		String insightFolderPath = InsightCacheUtility.getInsightCacheFolderPath(projectId, projectName, rdbmsId,
				params);

		// validate base path
		String expectedPath = Utility
				.normalizePath(projectDir + fileSeparator
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

	@Test
	public void testCacheInsight() throws Exception {
		// insight folder test vars
		String rdbmsId = "rdbmsId";
		String projectId = "projectId";
		String projectName = "projectName";

		Path version = projectDir.resolve(projectName + "__" + projectId).resolve("app_root").resolve("version");
		Files.createDirectories(version);

		Insight in = new Insight();
		in.setRdbmsId(rdbmsId);
		in.setProjectId(projectId);
		in.setProjectName(projectName);

		// test method
		File zipFile = null;
		try (MockedStatic<SecurityInsightUtils> ignored = Mockito.mockStatic(SecurityInsightUtils.class)) {
			in.runPixel("Date();");
			zipFile = InsightCacheUtility.cacheInsight(in, new HashSet<>(), new HashMap<>());
		}
		// Create temp/test directory to validate the contents of the zipFile
		File testZipFolder = new File(tempDir + "/test");
		testZipFolder.mkdir();

		// Unzip files
		try (ZipInputStream zis = new ZipInputStream(new FileInputStream(zipFile))) {
			ZipEntry entry;
			while ((entry = zis.getNextEntry()) != null) {
				Path filePath = testZipFolder.toPath().resolve(entry.getName());
				if (entry.isDirectory()) {
					Files.createDirectories(filePath);
				} else {
					Files.createDirectories(filePath.getParent());
					try (BufferedOutputStream bos = new BufferedOutputStream(Files.newOutputStream(filePath))) {
						byte[] buffer = new byte[1024];
						int len;
						while ((len = zis.read(buffer)) > 0) {
							bos.write(buffer, 0, len);
						}
					}
				}
				zis.closeEntry();
			}
		}

		// List files in temp/test directory
		assertEquals(3, testZipFolder.listFiles().length);

		// check that files exist
		File versionFile = new File(tempDir + "/test" + "/.version");
		assertTrue(versionFile.exists());
		File insightCache = new File(tempDir + "/test" + "/InsightCache.json");
		assertTrue(insightCache.exists());
		File viewData = new File(tempDir + "/test" + "/ViewData.json");
		assertTrue(viewData.exists());

		// teardown
		Utility.getProject(projectId).close();
	}
	
	@Test
	public void testCacheInvalidInsight() throws Exception {
		// insight folder test vars
		String rdbmsId = null;
		String projectId = "projectId";
		String projectName = "projectName";
		Insight in = new Insight();
		in.setRdbmsId(rdbmsId);
		in.setProjectId(projectId);
		in.setProjectName(projectName);
		
		try {
			InsightCacheUtility.cacheInsight(in, null, null);
			fail();
		}
		catch (IOException e) {
			assertEquals("Cannot jsonify an insight that is not saved", e.getMessage());
		}
	}

	@Test
	public void testWriteInsightCacheVersion() throws Exception {
		File versionFilePath = new File(tempDir.toFile(), ".version");
		assertFalse(versionFilePath.exists());
		versionFilePath = InsightCacheUtility.writeInsightCacheVersion(versionFilePath.getAbsolutePath());
		assertTrue(versionFilePath.exists());

		String contents = FileUtils.readFileToString(versionFilePath);
		assertTrue(contents.startsWith(InsightCacheUtility.VERSION_HEADER));
	}

	@Test
	public void testAddToZipFile() throws Exception {
		// create file to add to zip
		File fileToAdd = new File(tempDir.toFile(), "hello.txt");
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

	//@Test
	public void testReadInsightCache() throws IOException {

		// insight folder test vars
		String rdbmsId = "rdbmsId";
		String projectId = "projectId";
		String projectName = "projectName";

		Path version = projectDir.resolve(projectName + "__" + projectId).resolve("app_root").resolve("version");
		Files.createDirectories(version);

		String myProjSmss = projectName + "__" + projectId + ".smss";
		File projSmss = new File(projectDir.toFile(), myProjSmss);

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

		Files.createDirectories(projectDir);
		// save prop file
		try (FileOutputStream out = new FileOutputStream(projSmss)) {
			projProps.store(out, "Project Properties");
		} catch (IOException e) {
			e.printStackTrace();
		}

		Insight in = new Insight();
		in.setRdbmsId(rdbmsId);
		in.setProjectId(projectId);
		in.setProjectName(projectName);
		DIHelper.getInstance().setProjectProperty(projectId + "_" + Constants.STORE,
				projSmss.getAbsolutePath());


		try (MockedStatic<SecurityInsightUtils> ignored = Mockito.mockStatic(SecurityInsightUtils.class)) {
			in.runPixel("Date();");

			try (MockedStatic<SecurityProjectUtils> ignored2 = Mockito.mockStatic(SecurityProjectUtils.class)) {
				in.getPixelList();
			}
			// test method
			InsightCacheUtility.cacheInsight(in, new HashSet<>(), new HashMap<>());
		}

		Insight newInsight = null;
		try (MockedStatic<SecurityInsightUtils> ignored2 = Mockito.mockStatic(SecurityInsightUtils.class);
				MockedStatic<SecurityProjectUtils> ignored = Mockito.mockStatic(SecurityProjectUtils.class)) {
			newInsight = InsightCacheUtility.readInsightCache(in);
		}
		// validate insight info is read
		assertEquals(projectId, newInsight.getProjectId());
		assertEquals(projectName, newInsight.getProjectName());

		Utility.getProject(projectId).close();
	}

	@Test
	public void testGetCachedInsightViewData() throws IOException {
		// insight folder test vars
		String rdbmsId = "rdbmsId";
		String projectId = "projectId";
		String projectName = "projectName";
		Insight in = new Insight();
		in.setRdbmsId(rdbmsId);
		in.setProjectId(projectId);
		in.setProjectName(projectName);
		in.runPixel("Date();");
		
		// cache the insight
		InsightCacheUtility.cacheInsight(in, new HashSet<>(), new HashMap<>());

		// test method
		Map<String, Object> viewData = InsightCacheUtility.getCachedInsightViewData(in, null);
		// validate insight info is read
		assertFalse(viewData.isEmpty());
		assertFalse(viewData.get("insightID").toString().isEmpty());
	}

	@Test
	public void testDeleteCache() throws Exception {
		String fileSeparator = java.nio.file.FileSystems.getDefault().getSeparator();

		// insight folder test vars
		String rdbmsId = "rdbmsId";
		String projectId = "projectId";
		String projectName = "projectName";
		String myProjSmss = projectName + "__" + projectId + ".smss";
		File projSmss = new File(projectDir.toFile(), myProjSmss);

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

		Files.createDirectories(projectDir);
		// save prop file
		try (FileOutputStream out = new FileOutputStream(projSmss)) {
			projProps.store(out, "Project Properties");
		} catch (IOException e) {
			e.printStackTrace();
		}

		String myProjectFolderPath = Utility.normalizePath(projectDir + fileSeparator + projectName + "__"
				+ projectId);

		Insight in = new Insight();
		in.setRdbmsId(rdbmsId);
		in.setProjectId(projectId);
		in.setProjectName(projectName);
		DIHelper.getInstance().setProjectProperty(projectId + "_" + Constants.STORE,
				projSmss.getAbsolutePath());

		// set up base folder
		try (MockedStatic<SecurityInsightUtils> ignored = Mockito.mockStatic(SecurityInsightUtils.class)) {

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
	public void testUnzipFile() throws Exception {
		// create file to add to zip
		String fileContents = "hello world!!!!!!!!!";
		File fileToAdd = new File(tempDir.toFile(), "hello.txt");
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
		String newFile = tempDir.toFile().getAbsolutePath() + "\\output.txt";
		InsightCacheUtility.unzipFile(zip, "hello.txt", newFile);
		zip.close();

		// validations
		File unzippedFile = new File(newFile);
		assertTrue(unzippedFile.exists());
	}

}
