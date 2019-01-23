package prerna.junit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assume.assumeNoException;
import static org.junit.Assume.assumeThat;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;

import org.apache.commons.io.FileUtils;
import org.junit.Before;

public class PixelUnitWithDatabases extends PixelUnit {

	private static final String COLLEGE = "unit_test_college";
	private static final String MOVIE = "unit_test_movie";
	private static final String MOVIE_RDF = "unit_test_movie_rdf";
	
	private static final String METAMODEL_EXTENSION = "_metamodel.txt";
	
	@Before
	public void checkAssumptions() {
		checkTestDatabase(COLLEGE);
		checkTestDatabase(MOVIE);
		checkTestDatabase(MOVIE_RDF);
	}
	
	public void checkTestDatabase(String alias) {
		String appId = aliasToAppId.get(alias);
		String pixel = "GetDatabaseMetamodel(database=[\"" + appId + "\"]);";
		try {
			String expectedJson = FileUtils.readFileToString(Paths.get(TEST_DATA_DIRECTORY, alias + METAMODEL_EXTENSION).toFile());
			Object result = compareResult(pixel, expectedJson, true);
			assumeThat(result, is(equalTo(new HashMap<>())));
		} catch (IOException e) {
			LOGGER.error("Error: ", e);
			assumeNoException(e);
		}
	}
	
}
