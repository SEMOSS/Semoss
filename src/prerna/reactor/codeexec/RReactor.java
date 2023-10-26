package prerna.reactor.codeexec;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.om.Insight;
import prerna.om.InsightPanel;
import prerna.reactor.frame.r.AbstractRFrameReactor;
import prerna.reactor.frame.r.GenerateFrameFromRVariableReactor;
import prerna.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.ReactorSecurityManager;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ConstantDataTask;
import prerna.sablecc2.om.task.options.TaskOptions;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public final class RReactor extends AbstractRFrameReactor {
	
	private static transient SecurityManager defaultManager = System.getSecurityManager();
	private static final String CLASS_NAME = RReactor.class.getName();
	
	@Override
	public NounMetadata execute() {
		String disable_terminal =  DIHelper.getInstance().getProperty(Constants.DISABLE_TERMINAL);
		if(disable_terminal != null && !disable_terminal.isEmpty() ) {
			if(Boolean.parseBoolean(disable_terminal)) {
				throw new IllegalArgumentException("Terminal and user code execution has been disabled.");
			}
		}
		
		//check if r is disabled
		String disable_r_terminal =  DIHelper.getInstance().getProperty(Constants.DISABLE_R_TERMINAL);
		if(disable_r_terminal != null && !disable_r_terminal.isEmpty() ) {
			 if(Boolean.parseBoolean(disable_r_terminal)) {
					throw new IllegalArgumentException("R terminal has been disabled.");
			 }
		}

		// if it first time..
		// get the meta synchronized
		Logger logger = getLogger(CLASS_NAME);
		this.rJavaTranslator = this.insight.getRJavaTranslator(logger);
		rJavaTranslator.startR();
		boolean smartSync = (insight.getProperty("SMART_SYNC") != null) && insight.getProperty("SMART_SYNC").equalsIgnoreCase("true");
		// forcing smartSync to be true all the time
		smartSync = true;
		if(smartSync) {
			// see if the var has been set for first time
			if(insight.getProperty("FIRST_SYNC") == null) {
				smartSync(rJavaTranslator);
				insight.getVarStore().put("FIRST_SYNC", new NounMetadata("True", PixelDataType.CONST_STRING));
			}
		}

		ReactorSecurityManager tempManager = new ReactorSecurityManager();
		tempManager.addClass(CLASS_NAME);
		System.setSecurityManager(tempManager);
		
		// set the code variable for the ICodeExecution interface
		String code = Utility.decodeURIComponent(this.curRow.get(0).toString());
		logger.info("Execution r script: " + code);
		this.addExecutedCode(code);

		// bifurcation for ggplot
		if(code.startsWith("ggplot")) {
			return handleGGPlot(code);
		}
		
		//capture.output(tryCatch({
		//+ print("monkeshwaran")}, error = function(e){"error"; e$message}),
		//+ file="c:/users/pkapaleeswaran/workspacej3/r.out")

		String output = rJavaTranslator.runRAndReturnOutput(code, insight.getUser().getVarMap());
		List<NounMetadata> outputs = new ArrayList<>(1);
		outputs.add(new NounMetadata(output, PixelDataType.CONST_STRING));
		
		// set back the original security manager
		tempManager.removeClass(CLASS_NAME);
		System.setSecurityManager(defaultManager);	
		
		if(smartSync) {
			if(smartSync(rJavaTranslator)) {
				outputs.add(new NounMetadata(this.insight.getCurFrame(), PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE));
			}
		}
		return new NounMetadata(outputs, PixelDataType.CODE, PixelOperationType.CODE_EXECUTION);
	}
		
	public NounMetadata handleGGPlot(String ggplotCommand) {
		// I need to see how to get this to temp
		boolean newWindow = true;
		String panelId = "new_ggplot_panel";
		String fileName = Utility.getRandomString(6);
		String dir = insight.getUserFolder() + "/Temp";
		dir = dir.replaceAll("\\\\", "/");
		File tempDir = new File(dir);
		if(!tempDir.exists())
			tempDir.mkdir();

		ITableDataFrame frame = insight.getCurFrame();
		NounMetadata newFrameVar = null;
		if(frame == null)
		{
			// user is coming in for the first time
			// create the variable here
			// ggplot(framename, ... )
			String frameName = ggplotCommand.replace("ggplot(", "");
			frameName = frameName.split(",")[0].trim();
			GenRowStruct grs = new GenRowStruct();
			grs.add(frameName, PixelDataType.CONST_STRING);
			this.store.addNoun(ReactorKeysEnum.VARIABLE.getKey(), grs);
			
			GenerateFrameFromRVariableReactor gfrv = new GenerateFrameFromRVariableReactor();
			gfrv.setInsight(insight);
			gfrv.setNounStore(this.store);
			gfrv.In();
			newFrameVar = gfrv.execute();
			
			frame = insight.getCurFrame();
		}
		String [] headers = frame.getColumnHeaders();
		
		StringBuilder ggplotter = new StringBuilder("{library(\"ggplot2\");"); // library(\"RCurl\");");

		
		String frameName = frame.getName();
		if(ggplotCommand.contains(frameName))
		{
			ggplotCommand = ggplotCommand.replace(frameName, "plotterframe");
		}
		ggplotter.append("plotterframe <- " + frameName + ";");
		//ggplotter.append("library(\"gganimate\");");

		// now it is just running the ggplotter
		String plotString = Utility.getRandomString(6);
		ggplotter.append(plotString + " <- " + ggplotCommand + ";");
		boolean animate = ggplotCommand.contains("animate");

		String format = "jpeg";
		if(animate)
			format = "gif";
		// now save it
		// file name
		String ggsaveFile = Utility.getRandomString(6);
		
		String ROOT = insight.getInsightFolder();

		ROOT = ROOT.replace("\\","/");	
		
		ggplotter.append("ROOT <- \"" + ROOT + "\";");
		
		ggplotter.append(ggsaveFile + " <- " + "paste(\"" + ROOT + "\",\"/" + ggsaveFile + "." +format +"\", sep=\"\"); ");

		if(!animate)
			ggplotter.append("ggsave(" + ggsaveFile + ");");
		else
			ggplotter.append("anim_save(" + ggsaveFile + ", " + plotString + ");");
		
		ggplotter.append("print(" + ggsaveFile + ")");

		ggplotter.append("}");

		Logger logger = getLogger(CLASS_NAME);
		AbstractRJavaTranslator rJavaTranslator = this.insight.getRJavaTranslator(logger);
		// run the ggplotter command
		System.err.println("Running ggplotter string.. " + ggplotter);
		String fName = rJavaTranslator.runRAndReturnOutput(ggplotter.toString());

		// get file name
		String retFile = rJavaTranslator.runRAndReturnOutput(ggsaveFile);

		String ggremove = "rm(" + ggsaveFile + ", txt);detach(\"package:ggplot2\", unload=FALSE);"; //detach(\"package:RCurl\", unload=FALSE)";
		if(animate)
			ggremove = ggremove + "detach(\"package:gganimate\", unload=FALSE);";

		// remove the variable
		rJavaTranslator.runRAndReturnOutput(ggsaveFile);
		ConstantDataTask cdt = new ConstantDataTask();
		// need to do all the sets
		cdt.setFormat("TABLE");

		// TaskOptions options = new TaskOptions(optionMap);
		// need to do all the sets
		cdt.setFormat("TABLE");
		
		// compose a headers info List of Map with 2 keys name, alias
		List <Map<String, Object>> headerInfo = new ArrayList<Map<String, Object>>();
		for(int headIndex = 0;headIndex < headers.length;headIndex++)
		{
			String thisHeader = headers[headIndex];
			Map thisMap = new HashMap();
			thisMap.put("header", thisHeader);
			thisMap.put("alias", thisHeader);
			headerInfo.add(thisMap);
		}
		
		cdt.setHeaderInfo(headerInfo);
		//cdt.setSortInfo(task.getSortInfo());
		//cdt.setId(task.getId());
		Map<String, Object> formatMap = new Hashtable<String, Object>();
		formatMap.put("type", "TABLE");
		cdt.setFormatMap(formatMap);
		
		//   {0={layout=GGPlot, alignment={ggplot=[], selectors=[MovieBudget, RevenueDomestic]}}}
		Map<String, Object> alignmentMap = new Hashtable<String, Object>();
		alignmentMap.put("ggplot", new String[] {});
		alignmentMap.put("selectors", headers);


		Map <String, Object> optionMap = new HashMap<String, Object>();
		optionMap.put("layout", "GGPlot");
		optionMap.put("alignment", alignmentMap);
		// leaving out the alignment
		//optionMap.put(key, value)
		Map <String, Object> panelMap = new HashMap<String, Object>();
		
		int panelNum = 0;
		if(insight.getVarStore().containsKey(panelId))
			panelNum = (Integer)insight.getVarStore().get(panelId).getValue();
		panelNum++;
		insight.getVarStore().put(panelId, new NounMetadata(panelNum, PixelDataType.CONST_INT));
		
		if(newWindow)
			panelMap.put(panelId+panelNum, optionMap);
		else
			panelMap.put(this.insight.getLastPanelId(), optionMap);
		
		TaskOptions options = new TaskOptions(panelMap);
		
		cdt.setTaskOptions(options);

		Map<String, Object> outputMap = new HashMap<String, Object>();

		// get the insight folder
		String IF = insight.getInsightFolder();
		retFile = retFile.split(" ")[1].replace("\"","").replace("$IF", IF);
		retFile = Utility.normalizePath(retFile);
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
		}

		if(animate)
			ggplotCommand = ggplotCommand + " + animate";

		ggplotCommand = ggplotCommand.replaceAll("\"", "\\\\\"");
		// remove the variable
		rJavaTranslator.runRAndReturnOutput(ggremove);

		String bin = sw.toString();

		
		outputMap.put("headers", headers);
		outputMap.put("rawHeaders", headers);
		outputMap.put("values", new String[]{bin});
		outputMap.put("ggplot", ggplotCommand);	
		outputMap.put("format", format);

		// set the output so it can give it
		cdt.setOutputData(outputMap);
		new File(retFile).delete();

		// delete the pivot later
		if(bin.length() > 0) {
			List<NounMetadata> retList = new ArrayList<>();
			InsightPanel daPanel = insight.getInsightPanel(panelId);
			//newWindow = daPanel != null;
			if(newWindow) {
				// need to fix the scehario where the panel id can be replaced
				// can make panel id random moving forward
				daPanel = new InsightPanel(panelId+panelNum, Insight.DEFAULT_SHEET_ID);
				this.insight.addNewInsightPanel(daPanel);
			
				NounMetadata noun = new NounMetadata(daPanel, PixelDataType.PANEL, PixelOperationType.PANEL_OPEN);
				retList.add(noun);
				retList.add( new NounMetadata(cdt, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA, PixelOperationType.FILE));
				if(newFrameVar != null) {
					retList.add(newFrameVar);
				}
				return new NounMetadata(retList, PixelDataType.VECTOR, PixelOperationType.VECTOR);
			}
			//return //new NounMetadata(cdt, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA, PixelOperationType.FILE);
		} 	
		//return new NounMetadata(cdt, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA, PixelOperationType.FILE);
		return null;
	}
	
	@Override
	public boolean isUserScript() {
		return true;
	}

}
