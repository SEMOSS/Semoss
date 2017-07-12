package prerna.sablecc2.reactor.export.job;

import prerna.sablecc2.om.GenRowStruct;

public class FormatReactor extends JobBuilderReactor {
	
	@Override
	protected void buildJob() {
		GenRowStruct type = getNounStore().getNoun("type");
		if(type != null && !type.isEmpty()) {
			String formatType = type.get(0).toString();
			job.setFormat(formatType);
		}
	}
}
