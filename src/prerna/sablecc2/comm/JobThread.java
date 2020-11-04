package prerna.sablecc2.comm;

import java.util.ArrayList;
import java.util.List;

import prerna.om.Insight;
import prerna.sablecc2.PixelRunner;

public class JobThread extends Thread {

	private JobStatus status = JobStatus.CREATED;
	private String jobId = null;
	
	private Insight insight = null;
	private PixelRunner runner = null;
	private List<String> pixel = null;
	
	public JobThread(String jobId) {
		this.jobId = jobId;
	}

	@Override
	public void run() {
		try {
			this.status = JobStatus.INPROGRESS;
			this.runner = insight.runPixel(pixel);
			this.status = JobStatus.STREAMING;
		}catch (Exception ex) {
			ex.printStackTrace();
			this.status = JobStatus.ERROR;
		}
	}
	
	public void addPixel(String pixel) {
		if(this.pixel == null) {
			this.pixel = new ArrayList<String>();
		}
		this.pixel.add(pixel);
	}

	public Insight getInsight() {
		return insight;
	}

	public void setInsight(Insight insight) {
		this.insight = insight;
	}

	public void setJobId(String jobId) {
		this.jobId = jobId;
	}

	public String getJobId() {
		return this.jobId;
	}

	public void setStatus(JobStatus status) {
		this.status = status;
	}
	
	public String getStatus() {
		return this.status.value;
	}
	
	public PixelRunner getRunner() {
		return runner;
	}
}
