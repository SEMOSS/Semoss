package prerna.sablecc2.reactor;

import java.util.HashSet;
import java.util.Set;

public class ProjectCustomReactorCompilator {

	private static Set<String> compiled = new HashSet<>();

	private ProjectCustomReactorCompilator() {
		
	}
	
	public static void setCompiled(String projectId) {
		compiled.add(projectId);
	}
	
	public static boolean isCompiled(String projectId) {
		return compiled.contains(projectId);
	}
	
	public static void reset(String projectId) {
		compiled.remove(projectId);
	}
	
}
