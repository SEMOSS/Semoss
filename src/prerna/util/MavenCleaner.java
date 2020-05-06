package prerna.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.FileUtils;

public class MavenCleaner {

//	private static final Logger logger = LogManager.getLogger(MavenCleaner.class);

	private static final String PATH = System.getProperty("user.home") + java.nio.file.FileSystems.getDefault().getSeparator() + ".m2";

	public static void main(String[] args) throws IOException {
		Set<Path> foldersToDelete = new HashSet<>();
		Files.walk(Paths.get(PATH))
        .filter(f -> f.toFile().getName().endsWith(".lastUpdated"))
        .forEach(f -> foldersToDelete.add(f.getParent()));
		foldersToDelete.forEach(f -> {
			try {
				FileUtils.deleteDirectory(f.toFile());
				System.out.println("Deleted " + f.toString());
//				logger.info("Deleted " + f.toString());
			} catch (IOException e) {
				e.printStackTrace();
//				logger.error("StackTrace: ", e);
			}
		});
	}
	
}
