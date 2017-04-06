package prerna.sablecc2.reactor.export.job;

public class AddFormatReactor extends JobBuilderReactor {

	@Override
	protected void buildJob() {
		String name = (String)getNounStore().getNoun("formatName").get(0);
		String formatType = (String)getNounStore().getNoun("type").get(0);
		job.addFormat(formatType, name);
	}
}
