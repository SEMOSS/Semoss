package prerna.testing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.ProcessBuilder.Redirect;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.testcontainers.containers.GenericContainer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ApiSemossTestEmailUtils {

	private static String MAILPIT_FOLDER = Paths.get(ApiTestsSemossConstants.TEST_BASE_DIRECTORY, "mailpit").toString();
	private static String MAILPIT_WINDOWS_EXE = Paths.get(MAILPIT_FOLDER, "mailpit.exe").toString();
	private static String MAILPIT_MAC_EXE = Paths.get(MAILPIT_FOLDER, "mailpit").toString();
	private static String MAILPIT_LOG = Paths.get(MAILPIT_FOLDER, "mailpit.log").toString();

	private static String OS = System.getProperty("os.name").toLowerCase();
	private static boolean isWin = false;
	static {
		isWin = (OS.indexOf("win") >= 0);
	}

	public static void addStartupTasks(List<Callable<Void>> tasks) {
		if (ApiSemossTestUtils.usingDocker()) {
			tasks.add(ApiSemossTestEmailUtils::startEmailDockerContainer);
		} else {
			tasks.add(ApiSemossTestEmailUtils::startEmailLocalServer);
		}
	}

	public static boolean serverRunning() {
		try {
			URL url = new URL("http://localhost:8025/api/v1/info");
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			int status = conn.getResponseCode();
			return status == 200;
		} catch (Exception e) {
			return false;
		}
	}

	public static void deleteAllEmails() throws IOException {
		URL url = new URL("http://localhost:8025/api/v1/messages");
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setRequestMethod("DELETE");
		int status = conn.getResponseCode();
		assertEquals(status, 200);
	}

	public static List<Map<String, Object>> getAllEmails() {
		List<Map<String, Object>> m = new ArrayList<>();
		try {
			URL url = new URL("http://localhost:8025/api/v1/messages");
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			int status = conn.getResponseCode();
			assertEquals(status, 200);
			InputStream is = conn.getInputStream();
			ObjectMapper om = new ObjectMapper();
			Map<String, Object> map = om.readValue(is, Map.class);
			m = (List<Map<String, Object>>) map.get("messages");
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.toString());
		}

		return m;
	}

	public static Map<String, Object> getEmail(String id) {
		Map<String, Object> m = new HashMap<>();
		try {
			URL url = new URL("http://localhost:8025/api/v1/message/" + id);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			int status = conn.getResponseCode();
			assertEquals(status, 200);
			InputStream is = conn.getInputStream();
			ObjectMapper om = new ObjectMapper();
			m = om.readValue(is, Map.class);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.toString());
		}

		return m;
	}

	public static Void startEmailDockerContainer() {
		GenericContainer<?> mailpit = new GenericContainer<>("axllent/mailpit").withExposedPorts(1025, 8025);
		List<String> portBindings = new ArrayList<>();
		portBindings.add("1025:1025");
		portBindings.add("8025:8025");
		mailpit.setPortBindings(portBindings);
		mailpit.start();
		return null;
	}

	public static Void startEmailLocalServer() throws IOException, InterruptedException {
		if (!serverRunning()) {
			String processStr = null;
			if (isWin) {
				processStr = MAILPIT_WINDOWS_EXE;
				if (Files.notExists(Paths.get(MAILPIT_WINDOWS_EXE))) {
					fail("mailpit.exe not located, please read the readme in the testfolder/mailpit directory");
				}
			} else {
				processStr = MAILPIT_MAC_EXE;
				if (Files.notExists(Paths.get(MAILPIT_MAC_EXE))) {
					fail("mailpit.exe not located, please read the readme in the testfolder/mailpit directory");
				}
			}

			ProcessBuilder pb = new ProcessBuilder(processStr);
			pb.directory(new File(MAILPIT_FOLDER));

			if (Files.exists(Paths.get(MAILPIT_LOG))) {
				Files.delete(Paths.get(MAILPIT_LOG));
			}

			File log = new File(MAILPIT_LOG);
			pb.redirectErrorStream(true);
			pb.redirectOutput(Redirect.appendTo(log));
			Process p = pb.start();

			int i = 0;
			boolean found = false;
			while (i < 10 && !found) {
				List<String> lines = Files.readAllLines(Paths.get(MAILPIT_LOG));
				if (lines.contains("starting server on")) {
					found = true;
				}
				i++;
				Thread.sleep(1000L);
			}

			Runtime.getRuntime().addShutdownHook(new Thread(() -> p.destroy()));
		}
		return null;
	}

}
