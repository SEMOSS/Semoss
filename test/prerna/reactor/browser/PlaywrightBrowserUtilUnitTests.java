package prerna.reactor.browser;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PlaywrightBrowserUtilUnitTests {

	private PlaywrightBrowserUtil util = null;

	@BeforeEach
	void setup() {
		util = new PlaywrightBrowserUtil();
		util.initPlaywright();
	}

	@AfterEach
	void teardown() {
		try {
			util.close();
		} catch (Exception | Error e) {
			e.printStackTrace();
		}
	}

	@Test
	void navigateToPlaywrightWebsite() throws URISyntaxException {
		URL url = PlaywrightBrowserUtil.class.getResource("playwrightSearch.json");

		String file = Paths.get(url.toURI()).toString();

		util.processFile(file);
	}

	@Test
	void userSearches() throws URISyntaxException, InterruptedException, ExecutionException {
		InputStream oldIn = System.in;

		URL url = PlaywrightBrowserUtil.class.getResource("userSearch.json");

		String file = Paths.get(url.toURI()).toString();

		InputStream bais = new ByteArrayInputStream("!test".getBytes());
		System.setIn(bais);

		util.processFile(file);

		System.setIn(oldIn);

	}

}
