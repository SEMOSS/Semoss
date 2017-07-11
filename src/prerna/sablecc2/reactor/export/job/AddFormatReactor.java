package prerna.sablecc2.reactor.export.job;

public class AddFormatReactor extends JobBuilderReactor {

	@Override
	protected void buildJob() {
		String formatType = (String)getNounStore().getNoun("type").get(0);
		job.addFormat(formatType);
	}
}
