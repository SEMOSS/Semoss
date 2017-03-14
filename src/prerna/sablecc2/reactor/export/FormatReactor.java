package prerna.sablecc2.reactor.export;

import java.util.ArrayList;
import java.util.List;

import prerna.engine.api.IHeadersDataRow;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.reactor.AbstractReactor;

public class FormatReactor extends AbstractReactor {

	@Override
	public void In() {
		curNoun("all");
	}

	@Override
	public Object Out() {
		formatData();
		return parentReactor;
	}

	@Override
	protected void mergeUp() {
		
	}

	@Override
	protected void updatePlan() {
		
	}
	
	private void formatData() {
		GenRowStruct allNouns = getNounStore().getNoun(NounStore.all); //('EXPORT_FUNCTION_NAME', 'EXPORT_TYPE'), ex: (TABLE, BAR), (JSON, WIDGET)
		String formatFunction = (String)allNouns.get(0);
		Formatter formatter = FormatFactory.getFormatter(formatFunction);
		List<IHeadersDataRow> rawData = getRawData();
		
		for(IHeadersDataRow nextData : rawData) {
			formatter.addData(nextData);
		}
		
		NounMetadata noun = new NounMetadata(formatter.getFormattedData(), "DATA");
		planner.addProperty("RESULT", "RESULT", noun);
		//grab the data we are working with
//		List<Formatter> formatters;
//		boolean hasParent = this.parentReactor != null;
//		if(hasParent && parentReactor.hasProp("FORMATTER")) {
//			formatters = (List<Formatter>)parentReactor.getProp("FORMATTER");
//		} else {
//			formatters = new ArrayList<>(3);			
//		}
//		
//		this.parentReactor.setProp("FORMATTER", formatters);
//		
//		formatters.add(formatter);
//		this.planner.addProperty("FORMATTER", "FORMATTER", formatters);
//		if(this.parentReactor != null) {
//			if(this.parentReactor.hasProp("JOB")) {
//				this.setProp("JOB", this.parentReactor.getProp("JOB"));
//			}
//		}
	}

	@Override
	public List<NounMetadata> getInputs() {
		// TODO Auto-generated method stub
		return null;
	}
	
	private List<IHeadersDataRow> getRawData() {
		return (List<IHeadersDataRow>)planner.getProperty("RESULT", "RESULT");
	}
}
