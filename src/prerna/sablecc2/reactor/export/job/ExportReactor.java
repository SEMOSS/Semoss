package prerna.sablecc2.reactor.export.job;

import java.util.List;
import java.util.Vector;

import prerna.sablecc2.om.GenRowStruct;

public class ExportReactor extends JobBuilderReactor {

	@Override
	protected void buildJob() {
		// TODO Auto-generated method stub
		List<String> targets = getTargets();
		String format = getFormatName();
		String options = getOptionsName();

		for(String target : targets) {
			job.addOutput(target, format, options);
		}
	}

	private List<String> getTargets() {
		return flushOutJobMetadata("target");
	}

	private String getFormatName() {
		List<String> formatNameList = flushOutJobMetadata("formatName");
		if(formatNameList != null && !formatNameList.isEmpty()) {
			return formatNameList.get(0);
		} else {
			return null;
		}
	}

	private String getOptionsName() {
		List<String> optionsNameList = flushOutJobMetadata("optionsName");
		if(optionsNameList != null && !optionsNameList.isEmpty()) {
			return optionsNameList.get(0);
		} else {
			return null;
		}
	}

	private List<String> flushOutJobMetadata(String key) {
		List<String> returnMetadata = new Vector<String>();
		// grab the key from the store
		GenRowStruct metaDataGrs = this.store.getNoun(key);
		if(metaDataGrs != null) {
			// grab all string inputs
			int numTargets = metaDataGrs.size();
			for(int targetIndex = 0; targetIndex < numTargets; targetIndex++) {
				returnMetadata.add(metaDataGrs.get(targetIndex).toString());
			}
		}
		return returnMetadata;
	}

}
