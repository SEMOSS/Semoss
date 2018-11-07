package prerna.rpa.config;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.quartz.InterruptableJob;
import org.quartz.JobDataMap;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import prerna.rpa.RPAProps;
import prerna.rpa.config.specific.anthem.ProcessWGSPReportsJobConfig;
import prerna.rpa.config.specific.anthem.RunKickoutAlgorithmJobConfig;

public abstract class JobConfig {

	private static final Logger LOGGER = LogManager.getLogger(JobConfig.class.getName());

	private static final String PROP_REGEX = "<prop>(.*?)</prop>";
	private static final String TEXT_REGEX = "<text>(.*?)</text>";
	private static final String BYPASS_REGEX = "<bypass>(.*?)</bypass>";
	
	private static final String SPLIT_REGEX = ";";
	
	// For parsing the dates
	private static final String DATE_FORMAT = "yyyy-MM-dd";
	private final SimpleDateFormat dateFormatter = new SimpleDateFormat(DATE_FORMAT); // Non-thread safe, so not static
	
	protected final JsonObject jobDefinition;

	protected JobDataMap jobDataMap = new JobDataMap();
	
	public JobConfig(JsonObject jobDefinition) {
		this.jobDefinition = jobDefinition;
	}
	
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
		case ISOLATED_JOB:
			return new IsolatedJobConfig(jobDefinition);
		case ETL_JOB:
			return new ETLJobConfig(jobDefinition);
		case EXECUTE_SQL_JOB:
			return new ExecuteSQLJobConfig(jobDefinition);
		case GENERATE_HASHED_PRIMKEY_JOB:
			return new GenerateHashedPrimkeyJobConfig(jobDefinition);
		case BAKE_PIE_JOB:
			return new BakePieJobConfig(jobDefinition);
		case EAT_PIE_JOB:
			return new EmptyJobConfig(jobDefinition);
		case JUDGE_PIES_JOB:
			return new EmptyJobConfig(jobDefinition);
		case CONDITIONAL_JOB:
			return new ConditionalJobConfig(jobDefinition);
		case ONE_COL_CONDITION_JOB:
			return new OneColConditionJobConfig(jobDefinition);
		case CREATE_INSIGHT_JOB:
			return new CreateInsightJobConfig(jobDefinition);
		case GET_FRAME_FROM_INSIGHT_JOB:
			return new EmptyJobConfig(jobDefinition); // Don't need to provide any keys
		case INSIGHT_RERUN_JOB:
			return new InsightsRerunCronJobConfig(jobDefinition);
		case RUN_PIXEL_JOB:
			return new RunPixelJobConfig(jobDefinition);
		case GET_SMTP_SESSION_JOB:
			return new GetSMTPSessionJobConfig(jobDefinition);
		case SEND_EMAIL_JOB:
			return new SendEmailJobConfig(jobDefinition);
		case JEDIS_TO_JDBC_JOB:
			return new JedisToJDBCJobConfig(jobDefinition);
		case PROCESS_WGSP_REPORTS_JOB:
			return new ProcessWGSPReportsJobConfig(jobDefinition);
		case RUN_KICKOUT_ALGORITHM_JOB:
			return new RunKickoutAlgorithmJobConfig(jobDefinition);
		default:
			throw new IllegalArgumentException("Job configuration not found.");
		}
	}
	
	public JobDataMap getJobDataMap() throws ParseConfigException, IllegalConfigException {
		populateJobDataMap();
		return jobDataMap;
	}

	// Abstract method to populate the job data map is specific to the particular to each kind of job
	protected abstract void populateJobDataMap() throws ParseConfigException, IllegalConfigException;
	
	protected ConfigurableJob getConfigurableJob() {
		return ConfigurableJob.getConfigurableJobFromJobClassName(jobDefinition.get(JobConfigKeys.JOB_CLASS_NAME).getAsString());
	}
	
	public Class<? extends InterruptableJob> getJobClass() {
		return getConfigurableJob().getJobClass();
	}

	// These are not always defined and that is okay
	// Will return null if not found; not always needed
	public String getJobName() throws ParseConfigException {
		return getString(JobConfigKeys.JOB_NAME);
	}
	
	public String getJobGroup() throws ParseConfigException {
		return getString(JobConfigKeys.JOB_GROUP);
	}
	
	public String getCronExpression() throws ParseConfigException {
		return getString(JobConfigKeys.JOB_CRON_EXPRESSION);
	}
	
	public boolean getTriggerOnLoad() throws ParseConfigException {
		boolean triggerOnLoad = false;
		if (jobDefinition.get(JobConfigKeys.TRIGGER_ON_LOAD) != null) {
			triggerOnLoad = jobDefinition.get(JobConfigKeys.TRIGGER_ON_LOAD).getAsBoolean();
		}
		return triggerOnLoad;

	}

	public boolean getStatus() throws ParseConfigException {
		return jobDefinition.get(JobConfigKeys.ACTIVE).getAsBoolean();
	}
	
	/**
	 * Puts a string value from the jobDefinition (JSON) into the job data map given
	 * the job input key.
	 * <p>
	 * Three formats for a string value in the job definition:<br>
	 * 1) {@code "<prop>property.name</prop>"} - Get the final String value from the
	 * rpa.properties file<br>
	 * 2) {@code "<text>filename.txt</text>"} - Get the final String value from the
	 * contents of a text file, optionally
	 * {@code "<text>filename.txt:a,b;c,d</text>"} where the string "a" will be
	 * replaced with the string "b", and "c" with "d", and so on<br>
	 * 3) {@code "value"} - The final String value is simply "value"<br>
	 *
	 * @param jobInputKey
	 *            the key as it appears in a job's data map
	 * @throws ParseConfigException if unable to read the string from a text file
	 */
	protected void putString(String jobInputKey) throws ParseConfigException {
		
		// The key as it appears in the json
		String jsonKey = ConfigUtil.getJSONKey(jobInputKey);
		if (bypass(jsonKey)) return;
		
		// Call the getString method then put
		jobDataMap.put(jobInputKey, getString(jobInputKey));
	}

	/**
	 * Retrieves a string value from the jobDefinition (JSON) given the job input
	 * key.
	 * <p>
	 * Three formats for a string value in the job definition:<br>
	 * 1) {@code "<prop>property.name</prop>"} - Get the final String value from the
	 * rpa.properties file<br>
	 * 2) {@code "<text>filename.txt</text>"} - Get the final String value from the
	 * contents of a text file, optionally
	 * {@code "<text>filename.txt:a,b;c,d</text>"} where the string "a" will be
	 * replaced with the string "b", and "c" with "d", and so on<br>
	 * 3) {@code "value"} - The final String value is simply "value"<br>
	 *
	 * @param jobInputKey
	 *            the key as it appears in a job's data map
	 * @return the string value associated with the job input key
	 * @throws ParseConfigException if unable to read the string from a text file
	 */
	protected String getString(String jobInputKey) throws ParseConfigException {
		
		// The key as it appears in the json
		String jsonKey = ConfigUtil.getJSONKey(jobInputKey);
		
		// Retrieve the string value from the job definition (JSON)
		JsonElement element = jobDefinition.get(jsonKey);
		String value;
		if (element != null){
			value = element.getAsString();
		}else{
			return null;
		}
		
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
	
	// TODO docs: javadoc to explain semi-colon delimited
	protected void putStringArray(String jobInputKey) throws ParseConfigException {
		
		// The key as it appears in the json
		String jsonKey = ConfigUtil.getJSONKey(jobInputKey);
		if (bypass(jsonKey)) return;

		// Put the split-string value from the job definition (JSON)
		String[] stringArray = getString(jsonKey).split(SPLIT_REGEX);
		jobDataMap.put(jobInputKey, stringArray);

	}
	
	protected void putStringSet(String jobInputKey) throws ParseConfigException {
		
		// The key as it appears in the json
		String jsonKey = ConfigUtil.getJSONKey(jobInputKey);
		if (bypass(jsonKey)) return;

		// Put the split-string value from the job definition (JSON)
		String[] stringArray = getString(jsonKey).split(SPLIT_REGEX);
		Set<String> stringSet = new HashSet<>();
		stringSet.addAll(Arrays.asList(stringArray));
		jobDataMap.put(jobInputKey, stringSet);
	}
	
	protected void putLong(String jobInputKey) {
		
		// The key as it appears in the json
		String jsonKey = ConfigUtil.getJSONKey(jobInputKey);
		if (bypass(jsonKey)) return;

		// Put the long value from the job definition (JSON)
		jobDataMap.put(jobInputKey, jobDefinition.get(jsonKey).getAsLong());
	}
	
	protected void putInt(String jobInputKey) {
		
		// The key as it appears in the json
		String jsonKey = ConfigUtil.getJSONKey(jobInputKey);
		if (bypass(jsonKey)) return;

		// Put the int value from the job definition (JSON)
		jobDataMap.put(jobInputKey, jobDefinition.get(jsonKey).getAsInt());
	}
	
	protected void putBoolean(String jobInputKey) {
		
		// The key as it appears in the json
		String jsonKey = ConfigUtil.getJSONKey(jobInputKey);
		if (bypass(jsonKey)) return;

		// Put the boolean value from the job definition (JSON)
		jobDataMap.put(jobInputKey, jobDefinition.get(jsonKey).getAsBoolean());
	}
	
	// TODO docs: note date format in javadoc
	protected void putDate(String jobInputKey) throws ParseConfigException {
		
		// The key as it appears in the json
		String jsonKey = ConfigUtil.getJSONKey(jobInputKey);
		if (bypass(jsonKey)) return;
		
		// Call the getString method, parse, then put
		String dateString = getString(jobInputKey);
		try {
			Date date = parseDate(dateString);
			jobDataMap.put(jobInputKey, date);
		} catch (ParseException e) {
			throw new ParseConfigException("Failed to parse " + dateString + " into " + DATE_FORMAT + " format."); 
		} 
	}
	
	public Date parseDate(String dateString) throws ParseException {
		return dateFormatter.parse(dateString);
	}
	
	private boolean bypass(String jsonKey) {
		boolean bypass = false;
		JsonElement element = jobDefinition.get(jsonKey);
		if (element.isJsonPrimitive()) {
			String value = element.getAsString();
			Matcher bypassMatcher = Pattern.compile(BYPASS_REGEX).matcher(value);
			if (bypassMatcher.matches()) {
				bypass = true;
				String message = bypassMatcher.group(1);
				LOGGER.info("Bypassing the key " + jsonKey + " - \"" + message + ".\"");
			}
		}
		return bypass;
	}
	
}
