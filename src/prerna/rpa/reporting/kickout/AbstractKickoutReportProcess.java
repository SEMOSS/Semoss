package prerna.rpa.reporting.kickout;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.rpa.db.jedis.JedisStore;
import prerna.rpa.hash.Hasher;
import prerna.rpa.reporting.AbstractReportProcess;
import prerna.rpa.reporting.ReportProcessingException;
import prerna.util.Constants;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

public abstract class AbstractKickoutReportProcess extends AbstractReportProcess {
	
	private static final Logger LOGGER = LogManager.getLogger(AbstractKickoutReportProcess.class.getName());
	
	private static final int MOD_SIZE = 100000;
			
	private final String reportTimestamp;

	private final String timestampsKey;
	private final String referenceKey;
	private final String timeseriesKey;
	private final String processedKey;
	private final String reportKey;
	
	private final boolean discardHeader;
	
	// Allow each process to have its own hasher,
	// as opposed to a static final one shared by all kickout report processes,
	// since the .hash(String text) method is blocking
	private final Hasher hasher = Hasher.getDefaultHasher(8);
	
	public AbstractKickoutReportProcess(String reportPath, String prefix, boolean discardHeader) throws ParseException {
		super(reportPath);
		reportTimestamp = determineReportTimestamp(reportName);
		timestampsKey = KickoutJedisKeys.timestampsKey(prefix);
		referenceKey = KickoutJedisKeys.referenceKey(prefix);
		timeseriesKey = KickoutJedisKeys.timeseriesKey(prefix);
		processedKey = KickoutJedisKeys.processedKey(prefix);
		reportKey = KickoutJedisKeys.reportKey(prefix, reportTimestamp);
		this.discardHeader = discardHeader;
	}

	@Override
	protected boolean wasAlreadyProcessed() {
		try (Jedis jedis = JedisStore.getInstance().getResource()) {
			return jedis.smembers(processedKey).contains(reportName);
		}
	}
	
	// prefix_yyyy-MM-dd_HH:mm:ss {id}
	// prefix_reference {id, csv data}
	// prefix_timestamps {yyyy-MM-dd_HH:mm:ss}
	// prefix_timeseries {yyyy-MM-dd_HH:mm:ss, count(id)}
	// prefix_processed {report}
	@Override
	protected void process() throws ReportProcessingException {		
		try (Jedis jedis = JedisStore.getInstance().getResource()) {
			
			// Read the report
			try(BufferedReader br = readReport()) {
				
				// Discard the header if desired
				if (discardHeader) {
					br.readLine();
				}
				
				// Loop through each line and process each record
				int nRecord = 0;
			    for(String line; (line = br.readLine()) != null; ) {
			    	
			    	// Get the formatted line
			    	String record = formatLine(line);
			    	
			    	// If there is no data for this line, then continue
			    	if (record == null) {
			    		continue;
			    	}
			    	
			    	// Get the hashed id
			    	String id = hasher.hash(record);
			    				    	
					// prefix_yyyy-MM-dd_HH:mm:ss {id}
					jedis.sadd(reportKey, id);
					
					// prefix_reference {id, csv data}
					// hsetnx will only add if not already present
			    	jedis.hsetnx(referenceKey, id, record);
			    	
			    	nRecord++;
					if (nRecord % MOD_SIZE == 0) {
						LOGGER.info("Total records processed thus for " + reportName + ": " + nRecord);
					}
			    }
				LOGGER.info("Inserted data into jedis for " + reportName + ". Total number of records: " + nRecord);
			} catch (FileNotFoundException e) {
				LOGGER.error(Constants.STACKTRACE, e);
				throw new ReportProcessingException("Failed to find the report " + reportName + ".");
			} catch (IOException e) {
				LOGGER.error(Constants.STACKTRACE, e);
				throw new ReportProcessingException("An exception occurred while reading lines in " + reportName + ".");
			}
						
	    	// prefix_timestamps {yyyy-MM-dd_HH:mm:ss}
			jedis.sadd(timestampsKey, reportTimestamp);
			
			// prefix_timeseries {yyyy-MM-dd_HH:mm:ss, count(id)}
			int countIds = jedis.smembers(reportKey).size();
			jedis.hset(timeseriesKey, reportTimestamp, Integer.toString(countIds));
			
			// prefix_processed {report}
			jedis.sadd(processedKey, reportName);
		} catch (JedisDataException e) {
			LOGGER.warn("Encountered a Jedis data exception while processing " + reportName + "; will attempt to handle the issue.");
			handleJedisDataException();
		}
	}

	@Override
	protected void failedToProcess() {
		LOGGER.info("Removing " + reportName + " data from jedis.");
		try (Jedis jedis = JedisStore.getInstance().getResource()) {
			
			// prefix_yyyy-MM-dd_HH:mm:ss {id}
			jedis.del(reportKey);

			// prefix_reference {id, csv data}
			// We can keep any ids that were added to the reference table
			
			// prefix_timestamps {yyyy-MM-dd_HH:mm:ss}
			jedis.srem(timestampsKey, reportTimestamp);
			
			// prefix_timeseries {yyyy-MM-dd_HH:mm:ss, count(id)}
			jedis.hdel(timeseriesKey, reportTimestamp);
			
			// prefix_processed {report}
			jedis.srem(processedKey, reportName);
		}
	}
	
	protected abstract String determineReportTimestamp(String reportName) throws ParseException;
	
	protected abstract BufferedReader readReport() throws ReportProcessingException;
	
	protected abstract String formatLine(String line);
		
}
