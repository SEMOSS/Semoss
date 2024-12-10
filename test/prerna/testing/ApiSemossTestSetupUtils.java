package prerna.testing;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ApiSemossTestSetupUtils {

	public static void setup(boolean parallel) throws Exception {
		if (parallel) {
			setupParallel();
		} else {
			setupSeq();
		}
	}

	private static void setupParallel() throws Exception {
		// TODO Auto-generated method stub
		List<Callable<Void>> tasks = getTasks();
		ExecutorService es = Executors.newCachedThreadPool();
		try {
			es.invokeAll(tasks);
		} catch (Exception e) {
			throw e;
		} finally {
			es.shutdown();
		}
	}

	private static void setupSeq() throws Exception {
		for (Callable<Void> t : getTasks()) {
			t.call();
		}
	}

	private static List<Callable<Void>> getTasks() {
		List<Callable<Void>> tasks = new ArrayList<>();
		ApiSemossTestEmailUtils.addStartupTasks(tasks);
		ApiSemossTestEngineUtils.addDBStartupTasks(tasks);
		return tasks;
	}

	public static void ensureTestFolderStructure() throws IOException {
		String testFolderBase = ApiTestsSemossConstants.TEST_BASE_DIRECTORY;
		Path project = Paths.get(testFolderBase, "project");
		Path function = Paths.get(testFolderBase, "function");
		Path model = Paths.get(testFolderBase, "model");
		Path storage = Paths.get(testFolderBase, "storage");
		Path vector = Paths.get(testFolderBase, "vector");
		Path venv = Paths.get(testFolderBase, "venv");
		
		List<Path> ps = new ArrayList<>();
		ps.add(project);
		ps.add(function);
		ps.add(model);
		ps.add(storage);
		ps.add(vector);
		ps.add(venv);
		
		for (Path p : ps) {
			if (Files.notExists(p) && !Files.isDirectory(p)) {
				Files.createDirectories(p);
			}			
		}
	}

}
