package api;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.codehaus.plexus.util.FileUtils;

import prerna.reactor.project.CreateProjectReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ApiSemossTestProjectUtils {
	
	private static Path PROJECT_CONFIG_FILE = Paths.get(BaseSemossApiTests.TEST_CONFIG_DIRECTORY.toString(), "projects.txt");
	private static List<String> CURRENT_PROJECTS = new ArrayList<>();
	private static List<String> CORE_PROJECTS = null;
	
	@SuppressWarnings("unchecked")
	public static String createProject(String projectName) {
		assertFalse(CURRENT_PROJECTS.contains(projectName));
		assertNotNull(projectName);
		String pixel = ApiSemossTestUtils.buildPixelCall(CreateProjectReactor.class, "project", projectName);
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		CURRENT_PROJECTS.add(projectName);
		Map<String, Object> ret = (Map<String, Object>) nm.getValue();
		String projectId = ret.get("project_id").toString();
		return projectId;
	}

	public static void clearNonCoreProjects() throws IOException {
		List<String> projectsToAvoid = getProjectsToAvoid();
		File f = Paths.get(BaseSemossApiTests.TEST_PROJECT_DIRECTORY).toFile();
		List<String> toDelete = new ArrayList<>();
		for (String s : f.list()) {
			boolean found = false;
			for (String c : projectsToAvoid) {
				if (s.toLowerCase().startsWith(c.toLowerCase())) {
					found = true;
					break;
				}
			}
			if (!found) {
				toDelete.add(s);
			}
		}
		
		for (String delete : toDelete) {
			Path p = Paths.get(BaseSemossApiTests.TEST_PROJECT_DIRECTORY.toString(), delete);
			if (Files.isDirectory(p)) {
				FileUtils.cleanDirectory(p.toFile());
				Files.delete(p);
			} else {
				Files.delete(p);
			}
		}	
	}

	private static List<String> getProjectsToAvoid() throws IOException {
		if (CORE_PROJECTS != null) {
			return CORE_PROJECTS;
		}
		
		CORE_PROJECTS = Files.readAllLines(PROJECT_CONFIG_FILE).stream().map(s -> s.trim()).filter(s -> !s.isEmpty())
				.collect(Collectors.toList());
		return CORE_PROJECTS;
	}

}
