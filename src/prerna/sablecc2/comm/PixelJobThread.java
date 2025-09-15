package prerna.sablecc2.comm;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;

import prerna.om.Insight;
import prerna.om.ThreadStore;
import prerna.sablecc2.PixelRunner;
import prerna.util.Constants;

public class PixelJobThread extends Thread {

	public static final String JOB_KEY = "$JOB_ID";

	private static final Logger logger = LogManager.getLogger(PixelJobThread.class);

	private PixelJobStatus status = PixelJobStatus.CREATED;
	private String jobId;
	private String sessionId;
	private String routeId;

	private Insight insight;
	private PixelRunner runner;
	private List<String> pixel;

	private Map<String, String> log4jContextMap;

	public PixelJobThread(String jobId, Insight insight, String sessionId, String routeId) {
		this.jobId = jobId;
		this.insight = insight;
		this.sessionId = sessionId;
		this.routeId = routeId;

		// capture parent thread MDC
		this.log4jContextMap = ThreadContext.getImmutableContext();
	}

	@Override
	public void run() {
		try (var ctx = org.apache.logging.log4j.CloseableThreadContext.putAll(this.log4jContextMap)) {
			// set ThreadStore
			ThreadStore.setInsightId(insight.getInsightId());
			ThreadStore.setSessionId(sessionId);
			ThreadStore.setRouteId(routeId);
			ThreadStore.setJobId(jobId);
			ThreadStore.setUser(insight.getUser());

			this.runner = new PixelRunner();
			try {
				this.status = PixelJobStatus.IN_PROGRESS;
				this.runner = insight.runPixel(this.runner, this.pixel);
				this.status = PixelJobStatus.PROGRESS_COMPLETE;
			} catch (Exception ex) {
				logger.error(Constants.STACKTRACE, ex);
				this.status = PixelJobStatus.ERROR;
			}
		}
	}

	@Override
	public void interrupt() {
		super.interrupt();
		if (this.runner != null) {
			this.runner.interrupt();
		}
	}

	public void addPixel(String pixel) {
		if (this.pixel == null) {
			this.pixel = new ArrayList<>();
		}
		this.pixel.add(pixel);
	}

	public String getJobId() {
		return this.jobId;
	}

	public void setJobId(String jobId) {
		this.jobId = jobId;
	}

	public Insight getInsight() {
		return this.insight;
	}

	public PixelJobStatus getPixelJobStatus() {
		return this.status;
	}

	public void setStatus(PixelJobStatus status) {
		this.status = status;
	}

	public String getStatus() {
		return this.status.getValue();
	}

	public PixelRunner getRunner() {
		return runner;
	}
}
