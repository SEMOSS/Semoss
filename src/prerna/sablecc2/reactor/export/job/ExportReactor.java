package prerna.sablecc2.reactor.export.job;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.reactor.AbstractReactor;

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
		List<Object> targets = getNounStore().getNoun("target").getColumnsOfType(PkslDataTypes.CONST_STRING);
		List<String> retTargets = new ArrayList<>();
		for(Object target : targets) {
			retTargets.add((String)target);
		}
		return retTargets;
	}
	
	private String getFormatName() {
		List<Object> formatName = getNounStore().getNoun("formatName").getColumnsOfType(PkslDataTypes.CONST_STRING);
		return (String)formatName.get(0);
	}
	
	
	private String getOptionsName() {
		List<Object> optionsName = getNounStore().getNoun("optionsName").getColumnsOfType(PkslDataTypes.CONST_STRING);
		return (String)optionsName.get(0);
	}
}
