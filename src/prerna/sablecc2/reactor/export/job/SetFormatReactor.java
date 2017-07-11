package prerna.sablecc2.reactor.export.job;

public class SetFormatReactor extends JobBuilderReactor {
	
	@Override
	protected void buildJob() {
		String formatType = (String)getNounStore().getNoun("type").get(0);
		job.setFormat(formatType);
	}	
}
