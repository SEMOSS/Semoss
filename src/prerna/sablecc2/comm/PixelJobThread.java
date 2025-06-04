package prerna.sablecc2.comm;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.om.Insight;
import prerna.sablecc2.PixelRunner;
import prerna.util.Constants;

public class PixelJobThread extends Thread {

	private static final Logger logger = LogManager.getLogger(PixelJobThread.class);

	private PixelJobStatus status = PixelJobStatus.CREATED;
	private String jobId = null;
	
	private Insight insight = null;
	private PixelRunner runner = null;
	private List<String> pixel = null;
	
	public PixelJobThread(String jobId) {
		this.jobId = jobId;
	}

	@Override
	public void run() {
		try {
			this.status = PixelJobStatus.IN_PROGRESS;
			this.runner = insight.runPixel(pixel);
			this.status = PixelJobStatus.PROGRESS_COMPLETE;
		} catch (Exception ex) {
			logger.error(Constants.STACKTRACE, ex);
			this.status = PixelJobStatus.ERROR;
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

	public void setStatus(PixelJobStatus status) {
		this.status = status;
	}
	
	public PixelJobStatus getPixelJobStatus() {
		return this.status;
	}
	
	public String getStatus() {
		return this.status.getValue();
	}
	
	public PixelRunner getRunner() {
		return runner;
	}
}
