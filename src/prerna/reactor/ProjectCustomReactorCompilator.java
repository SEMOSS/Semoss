package prerna.reactor;

import java.util.HashSet;
import java.util.Set;

public class ProjectCustomReactorCompilator {

	private static Set<String> compiled = new HashSet<>();
	private static Set<String> failed = new HashSet<>();

	private ProjectCustomReactorCompilator() {
		
	}
	
	public static void setCompiled(String projectId) {
		compiled.add(projectId);
		failed.remove(projectId);
	}
	
	public static void setFailed(String projectId) {
		compiled.remove(projectId);
		failed.add(projectId);
	}
	
	public static boolean needsCompilation(String projectId) {
		return !compiled.contains(projectId) && !failed.contains(projectId);
	}
	
	public static boolean isCompiled(String projectId) {
		return compiled.contains(projectId);
	}
	
	public static boolean isFailed(String projectId) {
		return failed.contains(projectId);
	}
	
	public static void reset(String projectId) {
		compiled.remove(projectId);
		failed.remove(projectId);
	}
	
}
