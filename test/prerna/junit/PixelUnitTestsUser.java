package prerna.junit;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class PixelUnitTestsUser extends PixelUnit {

	protected static final String USER_TESTS_CSV = Paths.get(TEST_RESOURCES_DIRECTORY, "user_tests.csv").toAbsolutePath().toString();
	
	// Needed for parameterized tests
	private String name;
	private String pixel;
	private String expectedJson;
	private boolean compareAll;
	private List<String> excludePaths;
	private boolean ignoreOrder;
	private boolean ignoreAddedDictionary;
	private boolean ignoreAddedIterable;
	private List<String> cleanTestDatabases;
	private boolean ignoreFailure;
	
	public PixelUnitTestsUser(String name, String pixel, String expectedJson, boolean compareAll, List<String> excludePaths, boolean ignoreOrder, boolean ignoreAddedDictionary, boolean ignoreAddedIterable, List<String> cleanTestDatabases, boolean ignoreFailure) {
		this.name = name;
		this.pixel = pixel;
		this.expectedJson = expectedJson;
		this.compareAll = compareAll;
		this.excludePaths = excludePaths;
		this.ignoreOrder = ignoreOrder;
		this.ignoreAddedDictionary = ignoreAddedDictionary;
		this.ignoreAddedIterable = ignoreAddedIterable;
		this.cleanTestDatabases = cleanTestDatabases;
		this.ignoreFailure = ignoreFailure;
	}
		
	@Parameters(name = "{index}: (user defined) test {0}")
	public static Collection<Object[]> getTestParams() {
		return PixelUnitTests.getTestParams(USER_TESTS_CSV);
	}
	
	@Test
	public void runTest() throws IOException {
		PixelUnitTests.runTest(this, name, pixel, expectedJson, compareAll, excludePaths, ignoreOrder, ignoreAddedDictionary, ignoreAddedIterable, cleanTestDatabases, ignoreFailure);
	}

}
