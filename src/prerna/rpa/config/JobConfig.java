package prerna.rpa.config;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.quartz.InterruptableJob;
import org.quartz.JobDataMap;
import prerna.rpa.RPAProps;

import com.google.gson.JsonObject;

public abstract class JobConfig {
	
	private static final String PROP_REGEX = "<prop>(.*?)</prop>";
	private static final String TEXT_REGEX = "<text>(.*?)</text>";
	
	public static JobConfig initialize(JsonObject jobDefinition) {
		
		// Switch based on the target job class
		String jobClassName = jobDefinition.get(JobConfigKeys.JOB_CLASS_NAME).getAsString();
		ConfigurableJob configurableJob = ConfigurableJob.getConfigurableJobFromJobClassName(jobClassName);
		switch (configurableJob) {
		case JOB_BATCH:
			return new JobBatchConfig(jobDefinition);
		case JOB_CHAIN:
			return new JobChainConfig(jobDefinition);
		case IF_JOB:
			return new IfJobConfig(jobDefinition);
		case ETL_JOB:
			return new ETLJobConfig(jobDefinition);
		case EXECUTE_SQL_JOB:
			return new ExecuteSQLJobConfig(jobDefinition);
		case GENERATE_HASHED_PRIMKEY_JOB:
			return new GenerateHashedPrimkeyJobConfig(jobDefinition);
		case BAKE_PIES_JOB:
			return new BakePiesJobConfig(jobDefinition);
		case JUDGE_PIES_JOB:
			return new JudgePiesJobConfig(jobDefinition);
		case INSIGHT_RERUN_JOB:
			return new InsightsRerunCronJobConfig(jobDefinition);
		default:
			throw new IllegalArgumentException("Job configuration not found.");
		}
	}

	// Abstract method to get the job data map is specific to the particular to each kind of job
	public abstract JobDataMap getJobDataMap() throws Exception;
	
	public static ConfigurableJob getConfigurableJob(JsonObject jobDefinition) {
		return ConfigurableJob.getConfigurableJobFromJobClassName(jobDefinition.get(JobConfigKeys.JOB_CLASS_NAME).getAsString());
	}
	
	public static Class<? extends InterruptableJob> getJobClass(JsonObject jobDefinition) {
		return getConfigurableJob(jobDefinition).getJobClass();
	}

	// These are not always defined and that is okay
	// Will return null if not found; not always needed
	public static String getJobName(JsonObject jobDefinition) {
		return jobDefinition.get(JobConfigKeys.JOB_NAME).getAsString();
	}
	
	public static String getJobGroup(JsonObject jobDefinition) {
		return jobDefinition.get(JobConfigKeys.JOB_GROUP).getAsString();
	}
	
	public static String getCronExpression(JsonObject jobDefinition) {
		return jobDefinition.get(JobConfigKeys.JOB_CRON_EXPRESSION).getAsString();
	}

	/**
	 * Retrieves a string value from the jobDefinition (JSON) given the job input
	 * key.
	 * <p>
	 * Three formats for a string value in the job definition:<br>
	 * 1) {@code "<prop>property.name</prop>"} - Get the final String value from the
	 * rpa.properties file<br>
	 * 2) {@code "<text>filename.txt</text>"} - Get the final String value from the
	 * contents of a text file, optionally {@code "<text>filename.txt:a,b;c,d</text>"}
	 * where the string "a" will be replaced with the string "b", and "c" with "d", and so on<br>
	 * 3) {@code "value"} - The final String value is simply "value"<br>
	 *
	 * @param jobDefinition
	 * @param jobInputKey
	 * @return the string value associated with the job input key
	 * @throws Exception
	 */
	protected static String getString(JsonObject jobDefinition, String jobInputKey) throws Exception {
		
		// The key as it appears in the json
		String jsonKey = ConfigUtil.getJSONKey(jobInputKey);
		
		// Retrieve the string value from the job definition (JSON)
		String value = jobDefinition.get(jsonKey).getAsString();
		
		// 1) "<prop>property.name</prop>"
		Matcher propMatcher = Pattern.compile(PROP_REGEX).matcher(value);
		if (propMatcher.matches()) {
			String propertyName = propMatcher.group(1);
			return RPAProps.getInstance().getProperty(propertyName);
		}
				
		// 2) "<text>filename.txt</text>"
		// Optionally <text>filename.txt:a,b;c,d</text>
		Matcher textFileMatcher = Pattern.compile(TEXT_REGEX).matcher(value);
		if (textFileMatcher.matches()) {
			
			// Either "filename.txt" or "filename.txt:a,b;c,d"
			String match = textFileMatcher.group(1);
			
			// If in the format <text>filename.txt:a,b;c,d</text>, then make replacements
			if (match.contains(":")) {
				String[] parsed = match.split(":");
				String textFileName = parsed[0];
				String textFileContents = ConfigUtil.readStringFromTextFile(textFileName);
				String[] replacements = parsed[1].split(";");
				for (String replacement : replacements) {
					String[] tuple = replacement.split(",");
					textFileContents = textFileContents.replaceAll(tuple[0], tuple[1]);
				}
				return textFileContents;
			} else {
				return ConfigUtil.readStringFromTextFile(match);
			}
		}

		// 3) "value"
		return value;
	}
	
	protected static long getLong(JsonObject jobDefinition, String jobInputKey) {
		
		// The key as it appears in the json
		String jsonKey = ConfigUtil.getJSONKey(jobInputKey);
		
		// Return the long value from the job definition (JSON)
		return jobDefinition.get(jsonKey).getAsLong();
	}
	
}
