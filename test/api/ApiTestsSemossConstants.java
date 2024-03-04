package api;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

import prerna.util.Constants;

public class ApiTestsSemossConstants {

	public static final String BASE_DIRECTORY = new File("").getAbsolutePath();
	public static final String TEST_RESOURCES_DIRECTORY = Paths.get(BASE_DIRECTORY, "test", "resources", "api")
			.toAbsolutePath().toString();

	public static final String TEST_BASE_DIRECTORY = new File("testfolder").getAbsolutePath();

	public static final String TEST_DB_DIRECTORY = Paths.get(TEST_BASE_DIRECTORY, "db").toAbsolutePath().toString();
	public static final String TEST_PROJECT_DIRECTORY = Paths.get(TEST_BASE_DIRECTORY, "project").toAbsolutePath()
			.toString();;
	public static final Path TEST_CONFIG_DIRECTORY = Paths.get(TEST_BASE_DIRECTORY, "testconfig");

	public static final Path BASE_RDF_MAP = Paths.get(BASE_DIRECTORY, "RDF_Map.prop");
	public static final Path TEST_RDF_MAP = Paths.get(TEST_RESOURCES_DIRECTORY, "RDF_Map.prop");

	public static final String LMD_SMSS = Paths.get(TEST_DB_DIRECTORY, Constants.LOCAL_MASTER_DB + ".smss")
			.toAbsolutePath().toString();
	public static final String SECURITY_SMSS = Paths.get(TEST_DB_DIRECTORY, Constants.SECURITY_DB + ".smss")
			.toAbsolutePath().toString();
	public static final String SCHEDULER_SMSS = Paths.get(TEST_DB_DIRECTORY, Constants.SCHEDULER_DB + ".smss")
			.toAbsolutePath().toString();
	public static final String THEMES_SMSS = Paths.get(TEST_DB_DIRECTORY, Constants.THEMING_DB + ".smss")
			.toAbsolutePath().toString();
	public static final String UTDB_SMSS = Paths.get(TEST_DB_DIRECTORY, Constants.USER_TRACKING_DB + ".smss")
			.toAbsolutePath().toString();

}
