package prerna.util.ga.reactors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.query.querystruct.QueryStruct2;
import prerna.query.querystruct.QueryStructConverter;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class VisualizationRecommendationReactor extends AbstractRFrameReactor{

	public VisualizationRecommendationReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.TASK.getKey()};
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		// get inputs 		
		String inputTask = this.keyValue.get(this.keysToGet[0]);
		
		// get qs from input task 
		BasicIteratorTask task = (BasicIteratorTask) ((insight.getTaskStore()).getTask(inputTask));
		QueryStruct2 qs = task.getQueryStruct();
		
		// convert qs to physical names
		ITableDataFrame frame = (ITableDataFrame) this.insight.getDataMaker();
		OwlTemporalEngineMeta meta = frame.getMetaData();
		qs = QueryStructConverter.getPhysicalQs(qs, meta);	
		
		// prep script components
		StringBuilder builder = new StringBuilder();
		String inputFrame = "inputFrame." + Utility.getRandomString(8);
		String dfStart = "rm(list=ls()) ;library(RJSONIO); " + inputFrame + " <- data.frame(dbname = character(), tblname = character(), colname = character(), stringsAsFactors = FALSE);";
		
		// iterate selectors and update data table builder section of R script
		List<IQuerySelector> selectors = qs.getSelectors();
		Map<String, String> aliasHash = new HashMap<String, String>();
		for (int i = 0; i < selectors.size(); i++) {
			IQuerySelector selector = selectors.get(i);
			String alias = selector.getAlias();
			String name = "";
			name = selector.getQueryStructName();
			List<String[]> dbInfo = meta.getDatabaseInformation(name);
			aliasHash.put(alias, name);
			int size = dbInfo.size();
			for (int j = 0; j < size; j++) {
				String[] engineQs = dbInfo.get(0);
				String db = engineQs[0];
				String conceptProp = engineQs[1];
				String table = conceptProp;
				String column = QueryStruct2.PRIM_KEY_PLACEHOLDER;
				if (conceptProp.contains("__")) {
					String[] conceptPropSplit = conceptProp.split("__");
					table = conceptPropSplit[0];
					column = conceptPropSplit[1];
				}
				// TODO: change nrow to counter 
				builder.append(inputFrame)
				.append("[nrow(")
				.append(inputFrame)
				.append(") + 1, ] <- c( \"")
				.append(db).append("\", \"")
				.append(table)
				.append("\", \"")
				.append(column)
				.append("\");");
			}
		}
       
		// add the execute predict viz and convert to json piece to script
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String outputJson = "json_" + Utility.getRandomString(8);
		String runPredictScripts = "source(\"" + baseFolder 
				+ "\\R\\Recommendations\\viz_tracking.r\") ; "
				+ "df<-read.csv(\"" + baseFolder
				+ "\\R\\Recommendations\\historicalData\\viz_history.csv\") ; r<-viz_recom(df," +inputFrame + ", \"Grid\"); " + outputJson + " <-toJSON(r, byrow = TRUE, colNames = TRUE);";
		runPredictScripts = runPredictScripts.replace("\\", "/");
		
		
		// combine script pieces and execute in R
		String script = dfStart + builder + runPredictScripts;
		this.rJavaTranslator.runR(script);
		
		// receive json string from R
		String json = this.rJavaTranslator.getString(outputJson + ";");

		// R json string to map object
		List<Object> jsonMap = new ArrayList<Object>();
		if (json != null) {
			try {
				// parse json here
				jsonMap = new ObjectMapper().readValue(json, List.class);
			} catch (IOException e) {
			}
		} else {
			return null;
		}
		
		return new NounMetadata(jsonMap, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}
}
