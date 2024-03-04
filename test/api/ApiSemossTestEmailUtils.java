package api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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

import org.testcontainers.containers.GenericContainer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ApiSemossTestEmailUtils {
	
	private static String MAILPIT_FOLDER = Paths.get(ApiTestsSemossConstants.TEST_BASE_DIRECTORY, "mailpit").toString();
	private static String MAILPIT_EXE = Paths.get(MAILPIT_FOLDER, "mailpit.exe").toString();
	private static String MAILPIT_LOG = Paths.get(MAILPIT_FOLDER, "mailpit.log").toString();
	
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
	
	public static void deleteAllEmails() {
		try {
			URL url = new URL("http://localhost:8025/api/v1/messages");
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("DELETE");
			int status = conn.getResponseCode();
			assertEquals(status, 200);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.toString());
		}
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
		GenericContainer<?> mailpit = new GenericContainer<>("axllent/mailpit")
				.withExposedPorts(1025, 8025);
		List<String> portBindings = new ArrayList<>();
		portBindings.add("1025:1025");
		portBindings.add("8025:8025");
		mailpit.setPortBindings(portBindings);
		mailpit.start();
		return null;
	}


	public static Void startEmailLocalServer() throws IOException, InterruptedException {
		if (!serverRunning()) {
			ProcessBuilder pb = new ProcessBuilder(MAILPIT_EXE);
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
