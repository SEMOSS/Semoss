package prerna.rpa.quartz.jobs.reporting.kickout.specific.anthem;

import java.text.ParseException;
import java.util.Date;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.UnableToInterruptJobException;

import prerna.rpa.quartz.CommonDataKeys;
import prerna.rpa.reporting.AbstractReportProcessor;
import prerna.rpa.reporting.ReportProcessingException;
import prerna.rpa.reporting.kickout.specific.anthem.WGSPReportProcess;
import prerna.rpa.reporting.kickout.specific.anthem.WGSPReportProcessor;
import prerna.util.Utility;

public class ProcessWGSPReportsJob implements org.quartz.InterruptableJob {

	private static final Logger logger = LogManager.getLogger(ProcessWGSPReportsJob.class);
	
	// Constants
	// These are optimized to this use case
	private static final String REGEX = "^DELTA_KO_RPT.*zip$";
	private static final int N_THREADS = 33;
	private static final long SHUTDOWN_TIMEOUT = 43200L;
	
	/** {@code String} */
	public static final String IN_PREFIX_KEY = CommonDataKeys.KICKOUT_PREFIX;
	
	/** {@code String} */
	public static final String IN_REPORT_DIRECTORY_KEY = ProcessWGSPReportsJob.class + ".reportDirectory";
	
	/** {@code Set<String>} */
	public static final String IN_IGNORE_SYSTEMS_KEY = ProcessWGSPReportsJob.class + ".ignoreSystems";
	
	/** {@code java.util.Date} */
	public static final String IN_IGNORE_BEFORE_DATE_KEY = ProcessWGSPReportsJob.class + ".ignoreBefore";
		
	/** {@code boolean} */
	public static final String OUT_STATUS_KEY = ProcessWGSPReportsJob.class + ".status";
	
	private String jobName;
	private AbstractReportProcessor reportProcessor;
	
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		
		////////////////////
		// Get inputs
		////////////////////
		JobDataMap dataMap = context.getMergedJobDataMap();
		jobName = context.getJobDetail().getKey().getName();
		String prefix = dataMap.getString(IN_PREFIX_KEY);
		String reportDirectory = dataMap.getString(IN_REPORT_DIRECTORY_KEY);
		@SuppressWarnings("unchecked")
		Set<String> ignoreSystems = (Set<String>) dataMap.get(IN_IGNORE_SYSTEMS_KEY);
		Date ignoreBefore = (Date) dataMap.get(IN_IGNORE_BEFORE_DATE_KEY);
		
		////////////////////
		// Do work
		////////////////////
		boolean status = false;
		try {
			reportProcessor = new WGSPReportProcessor(reportDirectory, s -> {
				try {
					return s.matches(REGEX) && !WGSPReportProcess.parseKickoutDate(s)
							.before(ignoreBefore);
				} catch (ParseException e) {
					logger.warn("Will not process " + Utility.cleanLogString(s) + "; unable to parse the kickout date.");
					return false;
				}
			}, N_THREADS, SHUTDOWN_TIMEOUT, prefix, ignoreSystems);
			status = reportProcessor.processAll();
		} catch (ReportProcessingException e) {
			throw new JobExecutionException("Failed to initialize the report processor for WGSP.");
		}
		
		////////////////////
		// Store outputs
		////////////////////
		dataMap.put(OUT_STATUS_KEY, status);
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {
		logger.warn(jobName + ": " + "The " + jobName + " job was interrupted. Will stop accepting reports, but will allow existing processes to finish execution.");
		if (reportProcessor != null) {
			
			// Attempt to gracefully shutdown threads
			// This method allows open threads to finish processing,
			// while blocking new threads
			reportProcessor.shutdownAndAwaitTermination();
		}
	}

}
