package prerna.rpa.jobrunr.jobs;

import org.jobrunr.jobs.lambdas.JobRequest;
import org.jobrunr.jobs.lambdas.JobRequestHandler;

public class JobRunrPixelExecutionJobRequest implements JobRequest {

	private String pixelScript;
	private String pixelParameters;
	private String userAccess;
	private String execId;
	private String jobId;
	private String jobGroup;
	private String jobName;

	public JobRunrPixelExecutionJobRequest() {
		this(null, null, null, null, null, null, null);
	}

	/**
	 * Create a new Pixel execution job request
	 * 
	 * @param pixelScript     The Pixel script/code to execute
	 * @param pixelParameters Parameters for the Pixel script
	 * @param userAccess      Encrypted user access token
	 * @param execId          Execution ID for tracking
	 * @param jobId           Job identifier
	 * @param jobGroup        Job group identifier
	 */
	public JobRunrPixelExecutionJobRequest(String pixelScript, String pixelParameters, String userAccess, String execId,
			String jobId, String jobGroup, String jobName) {
		this.pixelScript = pixelScript;
		this.pixelParameters = pixelParameters;
		this.userAccess = userAccess;
		this.execId = execId;
		this.jobId = jobId;
		this.jobGroup = jobGroup;
		this.jobName = jobName;
	}

	@Override
	public Class<? extends JobRequestHandler> getJobRequestHandler() {
		return JobRunrPixelExecutionJobHandler.class;
	}

	// Getters and Setters for JSON serialization

	public String getPixelScript() {
		return pixelScript;
	}

	public void setPixelScript(String pixelScript) {
		this.pixelScript = pixelScript;
	}

	public String getPixelParameters() {
		return pixelParameters;
	}

	public void setPixelParameters(String pixelParameters) {
		this.pixelParameters = pixelParameters;
	}

	public String getUserAccess() {
		return userAccess;
	}

	public void setUserAccess(String userAccess) {
		this.userAccess = userAccess;
	}

	public String getExecId() {
		return execId;
	}

	public void setExecId(String execId) {
		this.execId = execId;
	}

	public String getJobId() {
		return jobId;
	}

	public void setJobId(String jobId) {
		this.jobId = jobId;
	}

	public String getJobGroup() {
		return jobGroup;
	}

	public String getJobName() {
		return jobName;
	}

	public void setJobGroup(String jobGroup) {
		this.jobGroup = jobGroup;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	@Override
	public String toString() {
		return "JobRunrPixelExecutionJobRequest{" + "pixelScript='"
				+ (pixelScript != null ? pixelScript.substring(0, Math.min(50, pixelScript.length())) + "..." : "null")
				+ '\'' + ", execId='" + execId + '\'' + ", jobId='" + jobId + '\'' + ", jobGroup='" + jobGroup + '\''
				+ '}';
	}
}
