package prerna.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SemossDefaultEngines {

	private static final List<String> IGNORE_DATABASE_OWL = new ArrayList<>();
	static {
		IGNORE_DATABASE_OWL.add(Constants.LOCAL_MASTER_DB);
		IGNORE_DATABASE_OWL.add(Constants.SECURITY_DB);
		IGNORE_DATABASE_OWL.add(Constants.THEMING_DB);
		IGNORE_DATABASE_OWL.add(Constants.SCHEDULER_DB);
		IGNORE_DATABASE_OWL.add(Constants.USER_TRACKING_DB);
	}
	
	private static final List<String> DATABASE_GENERATED_OWL = new ArrayList<>();
	static {
		DATABASE_GENERATED_OWL.addAll(IGNORE_DATABASE_OWL);
		DATABASE_GENERATED_OWL.add(Constants.MODEL_INFERENCE_LOGS_DB);
		DATABASE_GENERATED_OWL.add(Constants.PROMPT_DB);
	}
	
	public static List<String> getIgnoreDatabaseOwlList() {
		return Collections.unmodifiableList(IGNORE_DATABASE_OWL);
	}
	
	public static List<String> getDatabasesWithGeneratedOwl() {
		return Collections.unmodifiableList(DATABASE_GENERATED_OWL);
	}
	
}
