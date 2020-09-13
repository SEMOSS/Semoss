package prerna.rpa.reporting;

import java.io.File;
import java.util.concurrent.Callable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.rpa.RPAUtil;
import redis.clients.jedis.exceptions.JedisDataException;

/**
 * Framework for processing a single report. {@link AbstractReportProcessor}
 * asynchronously processes several reports at a time using this framework.
 * 
 * @author tbanach
 *
 */
public abstract class AbstractReportProcess implements Callable<Integer> {

	private static final Logger LOGGER = LogManager.getLogger(AbstractReportProcess.class.getName());
	
	protected final String reportPath;
	protected final File reportFile;
	protected final String reportName;
	
	private JedisDataExceptionHandler handler;
	
	/**
	 * @param reportPath - the full path to the report
	 */
	public AbstractReportProcess(String reportPath) {
		this.reportPath = reportPath;
		reportFile = new File(reportPath);
		reportName = reportFile.getName();
	}

	/**
	 * First checks whether this report has been processed; if not, attempts to
	 * process it using the abstract {@link #process()} method. If
	 * {@link #process()} throws a {@link ReportProcessingException}, then the
	 * {@link #failedToProcess()} method is called as an attempt to cleanup.
	 * 
	 * @return Integer - status code of -1 = error, 0 = already processed, 1 =
	 *         processed successfully
	 */
	@Override
	public Integer call() {
		LOGGER.info("Processing " + reportName + ".");

		// Message to display when the report will not be processed
		String willNotProcessMessage = "Will not process " + reportName + ". ";
		
		// Determine whether the report has already been processed
		boolean alreadyProcessed = wasAlreadyProcessed();
		
		// If not already processed, then process it
		if (alreadyProcessed) {
			LOGGER.info("The report " + reportName + " has already been processed.");
			LOGGER.info(willNotProcessMessage);
			return 0;
		} else {
			long startTime = System.currentTimeMillis();
			try {
				process();
				LOGGER.info("Completed processing the report " + reportName + " in " + RPAUtil.secondsSinceStartTime(startTime) + " seconds.");
				return 1;
			} catch (ReportProcessingException e) {
				LOGGER.warn("Failed to process " + reportName + ".", e);
				failedToProcess();
				return -1;
			}
		}
	}
		
	/**
	 * Checks whether this report was already processed. Implementations will
	 * usually need to query a persisted set of report names to check.
	 * 
	 * @return boolean - true if already processed
	 */
	protected abstract boolean wasAlreadyProcessed();
	
	/**
	 * Processes the report. Implementations will vary depending on the business
	 * logic in place.
	 * 
	 * @throws ReportProcessingException if an exception occurs while processing; results in a call to {@link #failedToProcess()}
	 */
	protected abstract void process() throws ReportProcessingException;

	/**
	 * Called whenever {@link #process()} throws an
	 * {@link ReportProcessingException}. Implementations should use this method to
	 * cleanup after a failure. For example, deleting data that was half written and
	 * removing processed tag so an attempt can be made later to reprocess. 
	 */
	protected abstract void failedToProcess();
	
	/**
	 * This is package-private because only AbstractReportProcessor should be
	 * setting this. Sets the handler needed for {@link #handleJedisDataException()}
	 * to bail out processes that experience a {@link JedisDataException} while
	 * processing reports.
	 * 
	 * @param handler
	 */
	void setJedisDataExceptionHandler(JedisDataExceptionHandler handler) {
		this.handler = handler;
	}
	
	/**
	 * Available to all subclasses as a method of resolving
	 * {@code JedisDataException}s that occur during processing. This runtime
	 * exception occurs when a Redis bgsave fails, locking further writes for all
	 * report processes. This method notifies a centralized handler of the problem,
	 * so that several threads do not try to resolve it at the same time. Holds
	 * until resolution.
	 */
	protected void handleJedisDataException() {
		handler.handleJedisDataException();
	}
}
