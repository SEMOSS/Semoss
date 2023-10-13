package prerna.reactor.scheduler.legacy;

import com.google.gson.annotations.SerializedName;

@Deprecated
public class OldJobs {
	@SerializedName("-jobName")
	private String jobName;
	@SerializedName("-jobGroup")
	private String jobGroup;
	@SerializedName("-jobCronExpression")
	private String jobCronExpression;
	@SerializedName("-jobClass")
	private String jobClass;
	@SerializedName("-active")
	private String active;
	@SerializedName("-userAccess")
	private String userAccess;
	@SerializedName("-jobTriggerOnLoad")
	private String jobTriggerOnLoad;
	
	private String pixel;
	private String parameters;
	private String hidden;

	public String getJobTriggerOnLoad() {
		return jobTriggerOnLoad;
	}

	public void setJobTriggerOnLoad(String jobTriggerOnLoad) {
		this.jobTriggerOnLoad = jobTriggerOnLoad;
	}

	public String getParameters() {
		return parameters;
	}

	public void setParameters(String parameters) {
		this.parameters = parameters;
	}

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public String getJobGroup() {
		return jobGroup;
	}

	public void setJobGroup(String jobGroup) {
		this.jobGroup = jobGroup;
	}

	public String getJobCronExpression() {
		return jobCronExpression;
	}

	public void setJobCronExpression(String jobCronExpression) {
		this.jobCronExpression = jobCronExpression;
	}

	public String getJobClass() {
		return jobClass;
	}

	public void setJobClass(String jobClass) {
		this.jobClass = jobClass;
	}

	public String getPixel() {
		return pixel;
	}

	public void setPixel(String pixel) {
		this.pixel = pixel;
	}

	public String getActive() {
		return active;
	}

	public void setActive(String active) {
		this.active = active;
	}

	public String getUserAccess() {
		return userAccess;
	}

	public void setUserAccess(String userAccess) {
		this.userAccess = userAccess;
	}

	public String getHidden() {
		return hidden;
	}

	public void setHidden(String hidden) {
		this.hidden = hidden;
	}
}
