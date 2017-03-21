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
		return parentReactor;
	}
	
	public Object execute() {
		GenRowStruct allNouns = getNounStore().getNoun(NounStore.all); //('EXPORT_FUNCTION_NAME', 'EXPORT_TYPE'), ex: (TABLE, BAR), (JSON, WIDGET)
		String formatFunction = (String)allNouns.get(0);
		Formatter formatter = FormatFactory.getFormatter(formatFunction);
		List<IHeadersDataRow> rawData = getRawData();
		
		for(IHeadersDataRow nextData : rawData) {
			formatter.addData(nextData);
		}
		
		NounMetadata noun = new NounMetadata(formatter.getFormattedData(), "FDATA");
//		planner.addVariable("$RESULT", noun);
		return noun;
	}

	@Override
	protected void mergeUp() {
		
	}

	@Override
	protected void updatePlan() {
		
	}

	@Override
	public List<NounMetadata> getInputs() {
		return null;
	}
	
	private List<IHeadersDataRow> getRawData() {
		NounMetadata dataNoun = (NounMetadata)getNounStore().getNoun("DATA").get(0);
		return (List<IHeadersDataRow>)dataNoun.getValue();
	}
}
