package prerna.util;

import java.util.ArrayList;
import java.util.Collection;
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

	private static final List<String> DATABASE_IGNORE_LOCALMASTER = new ArrayList<>();
	static {
		DATABASE_IGNORE_LOCALMASTER.add(Constants.LOCAL_MASTER_DB);
		DATABASE_IGNORE_LOCALMASTER.add(Constants.SECURITY_DB);
		DATABASE_IGNORE_LOCALMASTER.add(Constants.SCHEDULER_DB);
		DATABASE_IGNORE_LOCALMASTER.add(Constants.THEMING_DB);
		DATABASE_IGNORE_LOCALMASTER.add(Constants.USER_TRACKING_DB);
	}

	private static final List<String> DATABASE_IGNORE_SECURITY = new ArrayList<>();
	static {
		DATABASE_IGNORE_SECURITY.add(Constants.LOCAL_MASTER_DB);
		DATABASE_IGNORE_SECURITY.add(Constants.SECURITY_DB);
		DATABASE_IGNORE_SECURITY.add(Constants.SCHEDULER_DB);
		DATABASE_IGNORE_SECURITY.add(Constants.THEMING_DB);
		DATABASE_IGNORE_SECURITY.add(Constants.USER_TRACKING_DB);
	}

	public static List<String> getIgnoreDatabaseOwlList() {
		return Collections.unmodifiableList(IGNORE_DATABASE_OWL);
	}

	public static List<String> getDatabasesWithGeneratedOwl() {
		return Collections.unmodifiableList(DATABASE_GENERATED_OWL);
	}

	public static List<String> getDatabaseIgnoreLocalMaster() {
		return Collections.unmodifiableList(DATABASE_IGNORE_LOCALMASTER);
	}

	public static List<String> getDatabaseIgnoreSecurity() {
		return Collections.unmodifiableList(DATABASE_IGNORE_SECURITY);
	}

	/**
	 * Check if a string starts with any value within a collection
	 * @param strValue
	 * @param collection
	 * @return
	 */
	public static boolean valueStartsWith(String strValue, Collection<String> collection){
		for(String c : collection) {
			if(strValue.startsWith(c)) {
				return true;
			}
		}
		return false;
	}
}
