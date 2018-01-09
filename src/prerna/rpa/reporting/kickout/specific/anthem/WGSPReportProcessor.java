package prerna.rpa.reporting.kickout.specific.anthem;

import java.text.ParseException;
import java.util.Set;
import java.util.function.Predicate;

import prerna.rpa.reporting.AbstractReportProcess;
import prerna.rpa.reporting.AbstractReportProcessor;
import prerna.rpa.reporting.ReportProcessingException;

public class WGSPReportProcessor extends AbstractReportProcessor{

	private final String prefix;
	private final Set<String> ignoreSystems;
	
	public WGSPReportProcessor(String reportDirectory, Predicate<String> reportTester, int nThreads,
			long shutdownTimeout, String prefix, Set<String> ignoreSystems) throws ReportProcessingException {
		super(reportDirectory, reportTester, nThreads, shutdownTimeout);
		this.prefix = prefix;
		this.ignoreSystems = ignoreSystems;
	}

	@Override
	protected AbstractReportProcess giveProcess(String reportPath) throws ReportProcessingException {
		try {
			return new WGSPReportProcess(reportPath, prefix, ignoreSystems);
		} catch (ParseException e) {
			throw new ReportProcessingException("Failed to parse report timestamp for " + reportPath + ".", e);
		}
	}

}
