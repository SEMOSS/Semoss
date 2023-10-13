package prerna.reactor.export;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.io.FileUtils;

import prerna.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.reactor.task.TaskBuilderReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ConstantDataTask;
import prerna.util.Utility;

public class CollectVizNetworkReactor extends TaskBuilderReactor {

	/**
	 * This class is responsible for collecting data from a task and returning it
	 */

	private int limit = 0;
	private static final String VIZ_NETWORK = "vizNetwork";
	
	public CollectVizNetworkReactor() {
		this.keysToGet = new String[] { VIZ_NETWORK };
	}
	
	public NounMetadata execute() {
		organizeKeys();

		AbstractRJavaTranslator rJavaTranslator = this.insight.getRJavaTranslator(this.getLogger(this.getClass().getName()));
		rJavaTranslator.startR(); 
		rJavaTranslator.checkPackages(new String[] {"igraph", "visNetwork", "htmlwidgets"});

		// TODO: come back to how we handle custom selectors....
		// TODO: come back to how we handle custom selectors....
		// TODO: come back to how we handle custom selectors....
		// TODO: come back to how we handle custom selectors....

		this.task = getTask();
//		if(task instanceof BasicIteratorTask) {
//			SelectQueryStruct qs = ((BasicIteratorTask) task).getQueryStruct();
//			ITableDataFrame thisFrame = qs.getFrame();
//		}
		
		
		// we gotta generate the script
		// that wraps around the input being provided
		// which assumes that it will fit into the visNetwork method for generating the file
		String visNetworkCommand = this.keyValue.get(this.keysToGet[0]);
		
		String filePath = this.insight.getInsightFolder().replace("\\", "/");
		if(!filePath.endsWith("/")) {
			filePath += "/";
		}
		// if the folder doesn't exist yet - make it
		File filePathF = new File(filePath);
		if(!filePathF.exists() || !filePathF.isDirectory()) {
			filePathF.mkdirs();
		}
		String randomHtmlFile = "visNetwork_" + Utility.getRandomString(6) + ".html";
		// escape any quotes in the path - in case someone names things badly
		filePath = filePath.replace("\"", "\\\"");
		
		StringBuilder builder = new StringBuilder();
		builder.append("library('igraph');library('visNetwork');library('htmlwidgets');library('withr');");
		String randomGraphVar = "myGraph_" + Utility.getRandomString(6);
		builder.append(randomGraphVar).append(" <- visIgraph(").append(visNetworkCommand).append(");");
		builder.append("with_dir(\"").append(filePath).append("\", saveWidget(").append(randomGraphVar).append(", file=\"").append(randomHtmlFile).append("\"));");
		rJavaTranslator.executeEmptyR(builder.toString());
		
		filePath += randomHtmlFile;
        // update the File object to full file path
		filePathF = new File(filePath);
		// make sure this worked - html file should exist
		if(!filePathF.exists() || !filePathF.isFile()) {
			throw new IllegalArgumentException("Error occurred generating the network visualization. "
					+ "Please check your variable input for the igraph being used");
		}
		
		ConstantDataTask cdt = new ConstantDataTask();
		// TaskOptions options = new TaskOptions(optionMap);
		// need to do all the sets
		cdt.setFormat("TABLE");
		cdt.setTaskOptions(task.getTaskOptions());
		cdt.setHeaderInfo(task.getHeaderInfo());
		cdt.setSortInfo(task.getSortInfo());
		cdt.setId(task.getId());
		Map<String, Object> formatMap = new Hashtable<String, Object>();
		formatMap.put("type", "TABLE");
		cdt.setFormatMap(formatMap);

		Map<String, Object> outputMap = new HashMap<String, Object>();
		outputMap.put("headers", new String[] {});
		outputMap.put("rawHeaders", new String[] {});
		try {
			outputMap.put("values", new String[]{FileUtils.readFileToString(filePathF, StandardCharsets.UTF_8)});
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Error occurred reading the html file with full message: " + e.getMessage());
		}
		outputMap.put("visNetwork", visNetworkCommand);	
		outputMap.put("format", "html");
		// set the output so it can give it
		cdt.setOutputData(outputMap);

		// delete the pivot later
		return new NounMetadata(cdt, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA, PixelOperationType.FILE);
	}

	// keeping these methods for now.. I am not sure I require them
	
	@Override
	protected void buildTask() throws Exception {
		// if the task was already passed in
		// we do not need to optimize/recreate the iterator
		if(this.task.isOptimized()) {
			this.task.optimizeQuery(this.limit);
		}
	}
	
	@Override
	public List<NounMetadata> getOutputs() {
		List<NounMetadata> outputs = super.getOutputs();
		if(outputs != null && !outputs.isEmpty()) return outputs;
		
		outputs = new Vector<NounMetadata>();
		NounMetadata output = new NounMetadata(this.signature, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA);
		outputs.add(output);
		return outputs;
	}
	
	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(VIZ_NETWORK)) {
			return "The syntax to execute the vis network and store the visualization as a html";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
	
}
