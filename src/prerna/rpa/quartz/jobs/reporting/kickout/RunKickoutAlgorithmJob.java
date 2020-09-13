package prerna.rpa.quartz.jobs.reporting.kickout;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.UnableToInterruptJobException;

import prerna.rpa.quartz.CommonDataKeys;
import prerna.rpa.quartz.jobs.reporting.kickout.specific.anthem.ProcessWGSPReportsJob;
import prerna.rpa.reporting.kickout.KickoutAlgorithm;
import prerna.rpa.reporting.kickout.KickoutAlgorithmException;
import prerna.rpa.reporting.kickout.KickoutAlgorithms;

public class RunKickoutAlgorithmJob implements org.quartz.InterruptableJob {

	private static final Logger LOGGER = LogManager.getLogger(ProcessWGSPReportsJob.class.getName());
	
	/** {@code KickoutAlgorithm} - (Enum) */
	public static final String IN_KICKOUT_ALGORITHM = RunKickoutAlgorithmJob.class + ".kickoutAlgorithm";
	
	/** {@code String} */
	public static final String IN_PREFIX_KEY = CommonDataKeys.KICKOUT_PREFIX;
	
	/** {@code int} */
	public static final String IN_D_KEY = RunKickoutAlgorithmJob.class +  ".d";
	
	/** {@code String} */
	public static final String OUT_JEDIS_HASH_KEY = CommonDataKeys.JEDIS_HASH;
	
	private String jobName;
	
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		
		////////////////////
		// Get inputs
		////////////////////
		JobDataMap dataMap = context.getMergedJobDataMap();
		jobName = context.getJobDetail().getKey().getName();
		KickoutAlgorithm algorithm = (KickoutAlgorithm) dataMap.get(IN_KICKOUT_ALGORITHM);
		String prefix = dataMap.getString(IN_PREFIX_KEY);
		int d = dataMap.getInt(IN_D_KEY);
		
		////////////////////
		// Do work
		////////////////////
		KickoutAlgorithms runner = new KickoutAlgorithms(prefix);
		String jedisHash;
		try {
			jedisHash = runner.runAlgorithm(algorithm, d);
		} catch (KickoutAlgorithmException e) {
			throw new JobExecutionException(e);
		}
		
		////////////////////
		// Store outputs
		////////////////////
		dataMap.put(OUT_JEDIS_HASH_KEY, jedisHash);
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {
		LOGGER.warn("Received request to interrupt the " + jobName + " job. However, kickout algorithms are not interruptible.");
	}
	
}
