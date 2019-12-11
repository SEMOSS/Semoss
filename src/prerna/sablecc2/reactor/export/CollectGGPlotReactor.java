package prerna.sablecc2.reactor.export;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.io.FileUtils;

import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IRawSelectWrapper;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.BasicIteratorTask;
import prerna.sablecc2.om.task.ConstantDataTask;
import prerna.sablecc2.om.task.options.TaskOptions;
import prerna.sablecc2.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.reactor.task.TaskBuilderReactor;
import prerna.util.Utility;

public class CollectGGPlotReactor extends TaskBuilderReactor {

	/**
	 * This class is responsible for collecting data from a task and returning it
	 */

	private int limit = 0;
	
	public CollectGGPlotReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.GGPLOT.getKey(), ReactorKeysEnum.FORMAT.getKey()};
	}
	
	public NounMetadata execute() {
		
		organizeKeys();

		AbstractRJavaTranslator rJavaTranslator = this.insight.getRJavaTranslator(this.getLogger(this.getClass().getName()));
		rJavaTranslator.startR(); 
		
		String ggplotCommand = keyValue.get(keysToGet[0]) +"";
		
		String [] comTokens = ggplotCommand.split("\\+");
		StringBuilder newCommand = new StringBuilder();

		boolean animate = false;
		
		for(int tokenIndex = 0;tokenIndex < comTokens.length;tokenIndex++)
		{
			String command = comTokens[tokenIndex];
			String nextCommand = null;
			if(tokenIndex + 1 < comTokens.length)
				nextCommand = comTokens[tokenIndex + 1];
			if(!command.contains("animate"))
			{
				newCommand.append(command);
				if(nextCommand != null && !nextCommand.contains("animate"))
					newCommand.append(" + ");
			}
			else
				animate = true;
		}
		
		
		
		ggplotCommand = newCommand.toString();
		String format = "jpeg";
		if(animate)
			format = "gif";
		
		if(keyValue.containsKey(keysToGet[1]))
			format = keyValue.get(keysToGet[1]);
		
		this.task = getTask();
		
		// I neeed to get the basic iterator and then get types from there
		
		task.getMetaMap();
		SemossDataType[] sTypes = ((IRawSelectWrapper) ((BasicIteratorTask)(task)).getIterator()).getTypes();
		String[] headers = ((IRawSelectWrapper) ((BasicIteratorTask)(task)).getIterator()).getHeaders();
		
		Map<String, SemossDataType> typeMap = new HashMap<String, SemossDataType>();
		for(int i = 0; i < headers.length; i++) {
			typeMap.put(headers[i],sTypes[i]);
		}
		
		
		// I need to see how to get this to temp
		String fileName = Utility.getRandomString(6);
		String dir = insight.getUserFolder() + "/Temp";
		dir = dir.replaceAll("\\\\", "/");
		File tempDir = new File(dir);
		if(!tempDir.exists())
			tempDir.mkdir();
		String outputFile = dir + "/" + fileName + ".csv";
		Utility.writeResultToFile(outputFile, this.task, typeMap, ",");
		

		// need something here to adjust the types
		// need to move this to utilities 
		// will move it once we have figured it out
		String loadDT = fileName + " <- fread(\"" + outputFile + "\");";
		// adjust the types
		String adjustTypes = Utility.adjustTypeR(fileName, headers, typeMap);
		// run the job
		rJavaTranslator.runRAndReturnOutput(loadDT+adjustTypes);
		
		
		
		
		StringBuilder ggplotter = new StringBuilder("{library(\"ggplot2\");"); // library(\"RCurl\");");
		if(animate)
			ggplotter.append("library(\"gganimate\");");
		// get the frame
		// get frame name
		String table = fileName;
		
		// run the ggplot with this frame as the data
		// we will refer to it as the plotterframe
		ggplotter.append("plotterframe <- " + fileName + ";");
		
		// now it is just running the ggplotter
		String plotString = Utility.getRandomString(6);
		ggplotter.append(plotString + " <- " + ggplotCommand + ";");
		
		// now save it
		// file name
		String ggsaveFile = Utility.getRandomString(6);
		
		
		ggplotter.append(ggsaveFile + " <- " + "paste(ROOT,\"/" + ggsaveFile + "." +format +"\", sep=\"\"); ");
		
		if(!animate)
			ggplotter.append("ggsave(" + ggsaveFile + ");");
		else
			ggplotter.append("anim_save(" + ggsaveFile + ", " + plotString + ");");

		ggplotter.append("}");
		
		// run the ggplotter command
		rJavaTranslator.runRAndReturnOutput(ggplotter.toString());

		// get file name
		String retFile = rJavaTranslator.runRAndReturnOutput(ggsaveFile);

		// also try to encode it
		/*String encode = "base64Encode(readBin(" + ggsaveFile + " , \"raw\", file.info(" + ggsaveFile + ")[1, \"size\"]), \"txt\")";
		
		String dataURI = rJavaTranslator.runRAndReturnOutput(encode);
		System.out.println(dataURI);
		*/
		String ggremove = "rm(" + ggsaveFile + ", txt);detach(\"package:ggplot2\", unload=FALSE);"; //detach(\"package:RCurl\", unload=FALSE)";
		if(animate)
			ggremove = ggremove + "detach(\"package:gganimate\", unload=FALSE);";
		
		// remove the variable
		rJavaTranslator.runRAndReturnOutput(ggsaveFile);

		// remove the csv
		new File(outputFile).delete();
		
		// Need to figure out if I am trying to delete the image and URI encode it at some point.. 
		
		ConstantDataTask cdt = new ConstantDataTask();
		// need to do all the sets
		cdt.setFormat("TABLE");
		
		// I need to create the options here
		// Map optionMap = new HashMap<String, Object>();
		// optionMap.put(keysToGet[0], ggplotCommand);
		
		// TaskOptions options = new TaskOptions(optionMap);
		// need to do all the sets
		cdt.setFormat("TABLE");
		cdt.setTaskOptions(task.getTaskOptions());
		cdt.setHeaderInfo(task.getHeaderInfo());
		cdt.setSortInfo(task.getSortInfo());
		cdt.setId(task.getId());
		
		String outFile = retFile;
		
		Map<String, Object> outputMap = new HashMap<String, Object>();
		
		// get the insight folder
		String IF = insight.getInsightFolder();
		retFile = retFile.split(" ")[1].replace("\"","").replace("$IF", IF);
		
		StringWriter sw = new StringWriter();
		try
		{
			// read the file and populate it
			byte [] bytes = FileUtils.readFileToByteArray(new File(retFile));
			String encodedString = Base64.getEncoder().encodeToString(bytes);
			String mimeType = "image/png";
			mimeType = Files.probeContentType(new File(retFile).toPath());
			sw.write("<img src='data:" + mimeType + ";base64," + encodedString + "'>");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();		
		//sw.write("<html><body>");
		}


		if(animate)
			ggplotCommand = ggplotCommand + " + animate";
		
		ggplotCommand = ggplotCommand.replaceAll("\"", "\\\\\"");
		// remove the variable
		rJavaTranslator.runRAndReturnOutput(ggremove);

		outputMap.put("headers", new String[] {});
		outputMap.put("rawHeaders", new String[] {});
		outputMap.put("values", new String[]{sw.toString()});
		//outputMap.put("values", new String[]{retFile});
		//outputMap.put("ggplot", "ggplot=[\"" + ggplotCommand + "\"]");	
		outputMap.put("ggplot", ggplotCommand);	

		// set the output so it can give it
		cdt.setOutputData(outputMap);
		new File(retFile).delete();
		// I dont think the filter information is required
		//cdt.setFilterInfo(task.getFilterInfo());
		// delete the pivot later
		return new NounMetadata(cdt, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA, PixelOperationType.FILE);
	}
	

	// keeping these methods for now.. I am not sure I require them
	
	@Override
	protected void buildTask() {
		// if the task was already passed in
		// we do not need to optimize/recreate the iterator
		if(this.task.isOptimized()) {
			this.task.optimizeQuery(this.limit);
		}
	}
	
	private TaskOptions genOrnamentTaskOptions() {
		if(this.subAdditionalReturn != null && this.subAdditionalReturn.size() == 1) {
			NounMetadata noun = this.subAdditionalReturn.get(0);
			if(noun.getNounType() == PixelDataType.ORNAMENT_MAP) {
				// we will use this map as task options
				TaskOptions options = new TaskOptions((Map<String, Object>) noun.getValue());
				options.setOrnament(true);
				return options;
			}
		}
		return null;
	}
	
	//returns how much do we need to collect
	private int getTotalToCollect() {
		// try the key
		GenRowStruct numGrs = store.getNoun(keysToGet[1]);
		if(numGrs != null && !numGrs.isEmpty()) {
			return ((Number) numGrs.get(0)).intValue();
		}
		
		// try the cur row
		List<Object> allNumericInputs = this.curRow.getAllNumericColumns();
		if(allNumericInputs != null && !allNumericInputs.isEmpty()) {
			return ((Number) allNumericInputs.get(0)).intValue();
		}
		
		// default to 500
		return 500;
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
		if (key.equals(ReactorKeysEnum.LIMIT.getKey())) {
			return "The number to collect";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
	

	// encoding
	// uses RCurl
	// txt <- base64Encode(readBin(tf1, "raw", file.info(tf1)[1, "size"]), "txt")
	// tf1 - is just the file location
	// https://stackoverflow.com/questions/33409363/convert-r-image-to-base-64/36707831?noredirect=1#comment61003382_36707831
	
	
}
