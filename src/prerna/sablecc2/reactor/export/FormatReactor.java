package prerna.sablecc2.reactor.export;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IHeadersDataRow;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PkslDataTypes;
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
		Formatter formatter = null;

		//TODO get data source
		GenRowStruct dataStruct = getNounStore().getNoun("dataSource");
		String dataSource = null;
		if (dataStruct != null) {
			NounMetadata dataSourceNoun = (NounMetadata) dataStruct.get(0);
			dataSource = "";
		}

		// get data format type
		GenRowStruct type = getNounStore().getNoun("type");
		if (type != null) {
			Vector<String> formatVector = type.getAllColumns();
			String format = formatVector.get(0);
			formatter = FormatFactory.getFormatter(format);
		}

		//add data to formatter 
		List<IHeadersDataRow> rawData = getRawData();
		if (formatter != null) {
			for (IHeadersDataRow nextData : rawData) {
				formatter.addData(nextData);
			}
		}

		// get chart type
		GenRowStruct widgetStruct = getNounStore().getNoun("widget");
		Vector<String> labelValues = null;
		Vector<String> xAxisValues = null;
		Vector<String> values = null;
		if (widgetStruct != null) {
			
			//TODO get chart labels			
			GenRowStruct labelStruct = getNounStore().getNoun("label");
			labelValues = labelStruct.getAllColumns(); 
			String label = labelValues.get(0);
			String [] keys = new String [] {label, ((ITableDataFrame) this.planner.getFrame()).getDataType(label).toString(), "label"};
			formatter.addHeader(keys);
			
			
			
			GenRowStruct valueStruct = getNounStore().getNoun("value");
			values = valueStruct.getAllColumns(); 
			//String value = values.get(0);
			
			for(String value : values ) {
				//TODO
//				formatter.addHeader(new String[] {value, ((ITableDataFrame) this.planner.getFrame()).getDataType(value).toString(), "value"});
			}
			
			//TODO
//			GenRowStruct xAxisStruct = getNounStore().getNoun("xAxis");
//			xAxisValues = xAxisStruct.getAllColumns(); 
//			String xAxis = labelValues.get(0);
		}
		
		
		HashMap<String, Object> formatVals = new HashMap<String, Object>();
		HashMap<String, Object> widgetVals = new HashMap<String, Object>();
		Vector<String> widgets = widgetStruct.getAllColumns();
		if (widgets != null && dataSource != null) {
			// TODO: index matching?
			for (String key : widgets) {
//				widgetVals.put(key, dataSource);
				widgetVals.put(key, "formattedData");
			}
		}
		
		formatVals.put("Widgets", widgetVals);
		HashMap<String, Object> dataVals = new HashMap<String, Object>();
		dataVals.put("formattedData", formatter.getFormattedData());
		formatVals.put("Data", dataVals);

		// TODO get job id
		formatVals.put("jobid", "test");

		NounMetadata noun = new NounMetadata(formatVals, PkslDataTypes.FORMATTED_DATA_SET);
		// planner.addVariable("$RESULT", noun);
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
		NounMetadata dataNoun = (NounMetadata) getNounStore().getNoun("DATA").get(0);
		return (List<IHeadersDataRow>) dataNoun.getValue();
	}
}
