package prerna.testing;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

import prerna.util.Constants;

public class ApiTestsSemossConstants {

	public static final String BASE_DIRECTORY = new File("").getAbsolutePath();
	public static final String TEST_RESOURCES_DIRECTORY = Paths.get(BASE_DIRECTORY, "test", "resources").toAbsolutePath().toString();

	public static final String TEST_BASE_DIRECTORY = new File("testfolder").getAbsolutePath();

	public static final String TEST_DB_DIRECTORY = Paths.get(TEST_BASE_DIRECTORY, "db").toAbsolutePath().toString();
	public static final String TEST_PROJECT_DIRECTORY = Paths.get(TEST_BASE_DIRECTORY, "project").toAbsolutePath().toString();
	public static final Path TEST_CONFIG_DIRECTORY = Paths.get(TEST_BASE_DIRECTORY, "testconfig");

	public static final Path BASE_RDF_MAP = Paths.get(BASE_DIRECTORY, "RDF_Map.prop");
	public static final Path TEST_RDF_MAP = Paths.get(TEST_RESOURCES_DIRECTORY, "RDF_Map.prop");

	public static final String LMD_SMSS = Paths.get(TEST_DB_DIRECTORY, Constants.LOCAL_MASTER_DB + ".smss").toAbsolutePath().toString();
	public static final String SECURITY_SMSS = Paths.get(TEST_DB_DIRECTORY, Constants.SECURITY_DB + ".smss").toAbsolutePath().toString();
	public static final String SCHEDULER_SMSS = Paths.get(TEST_DB_DIRECTORY, Constants.SCHEDULER_DB + ".smss").toAbsolutePath().toString();
	public static final String THEMES_SMSS = Paths.get(TEST_DB_DIRECTORY, Constants.THEMING_DB + ".smss").toAbsolutePath().toString();
	public static final String UTDB_SMSS = Paths.get(TEST_DB_DIRECTORY, Constants.USER_TRACKING_DB + ".smss").toAbsolutePath().toString();
	
	// default user info
	public static final String USER_NAME = "user1";	
	public static final String USER_EMAIL = "user1@example.com";	
	
	
	// constants for email api
	public static final String EMAIL_BCC = "Bcc";	
	public static final String EMAIL_TO = "To";	
	public static final String EMAIL_FROM = "From";	
	public static final String EMAIL_ADDRESS = "Address";	
	public static final String EMAIL_SUBJECT = "Subject";	
	public static final String EMAIL_ATTACHMENTS = "Subject";	
	public static final String EMAIL_MESSAGE_ID = "MessageID";	
	public static final String EMAIL_ID = "ID";	
	public static final String EMAIL_HTML = "HTML";	
	public static final String EMAIL_TEXT = "Text";	
	public static final String EMAIL_DATE = "Date";	
	public static final String EMAIL_READ = "Read";	//boolean
	
	// movie data
	public static final String MOVIE_CSV_FILE_NAME = "Movies.csv";
	public static final Path TEST_MOVIE_CSV_PATH = Paths.get(TEST_RESOURCES_DIRECTORY, MOVIE_CSV_FILE_NAME);
	public static final String MOVIE_TABLE_NAME = "Movies";
	public static final String TITLE = "Title";
	public static final String MOVIE_BUDGET = "MovieBudget";
	public static final String ROTTEN_TOMATOES_AUDIENCE = "RottenTomatoes_Critics";
	public static final String ROTTEN_TOMATOES_CRITICS = "RottenTomatoes_Audience";
	public static final String REVENUE_DOMESTIC = "Revenue_Domestic";
	public static final Object REVENUE_INTERNATIONAL = "Revenue_International";
	public static final String DIRECTOR = "Director";
	public static final String STUDIO = "Studio";
	public static final String GENRE = "Genre";
	public static final Object NOMINATED = "Nominated";





}
