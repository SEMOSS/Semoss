package prerna.sablecc2.reactor.export.job;

import java.util.List;
import java.util.Vector;

import prerna.sablecc2.om.GenRowStruct;

public class FormatReactor extends JobBuilderReactor {
	
	@Override
	protected void buildJob() {
		GenRowStruct type = getNounStore().getNoun("type");
		if(type != null && !type.isEmpty()) {
			String formatType = type.get(0).toString();
			job.setFormat(formatType);
		}
		
		GenRowStruct views = getNounStore().getNoun("views");
		if(views != null && !views.isEmpty()) {
			job.setViews(flushOutGrs(views));
		}
		
		GenRowStruct targets = getNounStore().getNoun("targets");
		if(targets != null && !targets.isEmpty()) {
			job.setTargets(flushOutGrs(targets));
		}
	}
	
	/**
	 * Used to iterate through a set of grs values to get the objects
	 * @param grs
	 * @return
	 */
	private List<String> flushOutGrs(GenRowStruct grs) {
		List<String> grsVals = new Vector<String>();
		int grsSize = grs.size();
		for(int i = 0; i < grsSize; i++) {
			grsVals.add(grs.get(i).toString());
		}
		
		return grsVals;
	}
}
