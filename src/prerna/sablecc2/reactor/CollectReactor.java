package prerna.sablecc2.reactor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import prerna.engine.api.IHeadersDataRow;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.reactor.export.FormatFactory;
import prerna.sablecc2.reactor.export.Formatter;

public class CollectReactor extends AbstractReactor{

	@Override
	public void In() {
		curNoun("all");
		printReactorTrace();
	}

	@Override
	public Object Out() {
		collectData();
		return parentReactor;
	}

	@Override
	protected void mergeUp() {
		
	}

	@Override
	protected void updatePlan() {
		
	}
	
	public void collectData() {
//		if(this.parentReactor != null) {
			Iterator<IHeadersDataRow> job = getJob();
//			List<Formatter> formatters = getFormatters();	
			int collectThisMany = getTotalToCollect();
			
			List<Object> values = new ArrayList<>(collectThisMany);
			int i = 0;
//			while(i < collectThisMany && job.hasNext()) {
//				IHeadersDataRow nextData = job.next();
//				values.add(nextData);
//				for(Formatter formatter : formatters) {
//					formatter.addData(nextData);
//				}
//				i++;
//			}
			
			while(i < collectThisMany && job.hasNext()) {
				IHeadersDataRow nextData = job.next();
				values.add(nextData);
				i++;
			}
			
			//collect the results
//			List<Object> values = new ArrayList<>();
//			for(Formatter formatter : formatters) {
//				values.add(formatter.getFormattedData());
//			}
			
//			Map retData;
//			if(this.planner.hasProperty("DATA", "DATA")) {
//				retData = (Map)this.planner.getProperty("DATA", "DATA");
//			} else {
//				retData = new HashMap<>();
//			}
//			
//			retData.put("jobOutput", values);
			
			this.planner.addProperty("RESULT", "RESULT", values);
//		}
	}
	
	private Iterator<IHeadersDataRow> getJob() {
		return (Iterator<IHeadersDataRow>)this.parentReactor.getProp("JOB");
	}
	
//	private List<Formatter> getFormatters() {
//		List<Formatter> formatters;
//		if(this.parentReactor.hasProp("FORMATTER")) {
//			formatters = (List<Formatter>)parentReactor.getProp("FORMATTER");
//		} else {
//			//one table by default
//			formatters = new ArrayList<>();
//			formatters.add(FormatFactory.getFormatter("Table"));
//		}
//		return formatters;
//	}
	
	private int getTotalToCollect() {
		GenRowStruct allNouns = getNounStore().getNoun(NounStore.all);
		Double collectThisMany = (Double)allNouns.get(0);
		return collectThisMany.intValue();
	}
}
