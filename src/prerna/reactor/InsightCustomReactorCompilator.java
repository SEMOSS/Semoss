package prerna.reactor;

import java.util.HashSet;
import java.util.Set;

import prerna.om.Insight;

public class InsightCustomReactorCompilator {

	private static Set<String> compiled = new HashSet<>();

	private InsightCustomReactorCompilator() {
		
	}
	
	public static void setCompiled(String key) {
		compiled.add(key);
	}
	
	public static boolean isCompiled(String key) {
		return compiled.contains(key);
	}
	
	public static void reset(String key) {
		compiled.remove(key);
	}
	
	public static String getKey(Insight in) {
		if(in.isSavedInsight()) {
			return in.getProjectId() + "-" + in.getRdbmsId();
		}
		return in.getInsightId();
	}
	
}
