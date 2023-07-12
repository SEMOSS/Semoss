package prerna.sablecc2.comm;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.om.Insight;
import prerna.sablecc2.PixelRunner;
import prerna.util.Constants;

public class JobThread extends Thread {

	private static final Logger logger = LogManager.getLogger(JobThread.class);

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
		} catch (Exception ex) {
			logger.error(Constants.STACKTRACE, ex);
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
		return this.status.getValue();
	}
	
	public PixelRunner getRunner() {
		return runner;
	}
}
