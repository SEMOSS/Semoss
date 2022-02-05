package prerna.sablecc2.reactor.frame.py;

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
import java.util.Vector;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ICodeExecution;
import prerna.algorithm.api.ITableDataFrame;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.ds.py.PyTranslator;
import prerna.ds.py.PyUtils;
import prerna.om.Insight;
import prerna.om.InsightPanel;
import prerna.om.Variable.LANGUAGE;
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

public class PyReactor extends AbstractPyFrameReactor implements ICodeExecution {
	
	private static transient SecurityManager defaultManager = System.getSecurityManager();
	private static final String CLASS_NAME = PyReactor.class.getName();
	// the code that was executed
	private String code = null;

	@Override
	public NounMetadata execute() {
		String disable_terminal =  DIHelper.getInstance().getProperty(Constants.DISABLE_TERMINAL);
		if(disable_terminal != null && !disable_terminal.isEmpty() ) {
			if(Boolean.parseBoolean(disable_terminal)) {
				throw new IllegalArgumentException("Terminal and user code execution has been disabled.");
			}
		}

		if(!PyUtils.pyEnabled()) {
			throw new IllegalArgumentException("Python is not enabled to use the following command");
		}
		Logger logger = getLogger(CLASS_NAME);

		this.code = Utility.decodeURIComponent(this.curRow.get(0).toString());
		int tokens = code.split("\\n").length;

		PyTranslator pyTranslator = this.insight.getPyTranslator();
		pyTranslator.setLogger(logger);
		//String output = pyTranslator.runPyAndReturnOutput(code);
		String output = null;
		
		if(code.startsWith("sns."))
			return handleSeaborn(code);
		
		if(AbstractSecurityUtils.securityEnabled()) {
			if(tokens > 1)
				output = pyTranslator.runPyAndReturnOutput(insight.getUser().getVarMap(), code) + "";
			else
				//output = pyTranslator.runScript(code) + "";
			output = pyTranslator.runScript(insight.getUser().getVarMap(), code) + "";

		} else {
			if(tokens > 1)
				output = pyTranslator.runPyAndReturnOutput(code) + "";
			else
				output = pyTranslator.runScript(code) + "";
		}
		List<NounMetadata> outputs = new Vector<NounMetadata>(1);
		outputs.add(new NounMetadata(output, PixelDataType.CONST_STRING));
		
		boolean smartSync = (insight.getProperty("SMART_SYNC") != null) && insight.getProperty("SMART_SYNC").equalsIgnoreCase("true");
		//forcing smart sync to true
		smartSync = true;

		if(smartSync) {
			// if this returns true
			if(smartSync(pyTranslator)) {
				outputs.add(new NounMetadata(this.insight.getCurFrame(), PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE));
			}
		}
		// call it here.. and if it return true
		// regenerate the metadata. 
		return new NounMetadata(outputs, PixelDataType.CODE, PixelOperationType.CODE_EXECUTION);
	}
	
	public NounMetadata handleSeaborn(String command) {
		//organizeKeys();
		
		String panelId = "new_seaborn_panel";

		PyTranslator pyt = this.insight.getPyTranslator();
		pyt.setLogger(this.getLogger(this.getClass().getName()));
		
		String format = "png";
		boolean newWindow = true;
		
		//if(keyValue.containsKey(keysToGet[1]))
		//	format = keyValue.get(keysToGet[1]);
		
		// I neeed to get the basic iterator and then get types from there
		// this is typically what we do on seaborn
		
		// import seaborn as sns
		// daplot = <Whatever the user enters>
		// daplot.savefig(location)
		// del daplot
		// del plotterframe
		// return output
		
		// need something here to adjust the types
		// need to move this to utilities 
		// will move it once we have figured it out
		
		// there is a third scenario that needs to be addressed i..e when the frame type is R
		
		ITableDataFrame frame = insight.getCurFrame();
		String type = frame.getFrameType().getTypeAsString();
		
		NounMetadata newFrameVar = null;
		if(frame == null) {
			// user is coming in for the first time
			// create the variable here
			// ggplot(framename, ... )
			String frameName = command.substring(command.indexOf("("));
			frameName = frameName.split(",")[0].trim();
			frameName = frameName.replace("data=", "").trim();
			GenRowStruct grs = new GenRowStruct();
			grs.add(frameName, PixelDataType.CONST_STRING);
			this.store.addNoun(ReactorKeysEnum.VARIABLE.getKey(), grs);
			
			GenerateFrameFromPyVariableReactor gfrv = new GenerateFrameFromPyVariableReactor();
			gfrv.setInsight(insight);
			gfrv.setNounStore(this.store);
			gfrv.In();
			newFrameVar = gfrv.execute();
			
			frame = insight.getCurFrame();
		}
		else if(frame != null && type.equalsIgnoreCase("R"))
		{
			// need to do the synchronization routine
			
		}
		
		String frameName = frame.getName();
		String assigner = "";
		if(command.contains(frameName))
		{
			command = command.replace(frameName, "plotterframe");
			assigner = "plotterframe = " + frameName;
		}

		
		String importSeaborn = "import seaborn as sns";
		String runPlot = "daplot = " + command;
		String seabornFile = Utility.getRandomString(6);
		String printFile = "print(saveFile)";
		String saveFileName = "saveFile = ROOT + '/" + seabornFile + "." + format + "'";
		String savePlot = "daplot.savefig(saveFile)";
		String removeSeaborn = "del(sns)";
		String removeSaveFile = "del(saveFile)";
		
		seabornFile = (String)pyt.runPyAndReturnOutput(assigner, importSeaborn, saveFileName, runPlot, savePlot, removeSeaborn, printFile, removeSaveFile);

		// get the insight folder
		String IF = insight.getInsightFolder();
		seabornFile = Utility.normalizePath(seabornFile.replace("$IF", IF));
		
		StringWriter sw = new StringWriter();
		String bin = null;
		try
		{
			// read the file and populate it
			byte [] bytes = FileUtils.readFileToByteArray(new File(seabornFile));
			String encodedString = Base64.getEncoder().encodeToString(bytes);
			String mimeType = "image/png";
			mimeType = Files.probeContentType(new File(seabornFile).toPath());
			sw.write("<img src='data:" + mimeType + ";base64," + encodedString + "'>");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();		
		}

		new File(seabornFile).delete();
		String [] headers = frame.getColumnHeaders();

		
		// Need to figure out if I am trying to delete the image and URI encode it at some point.. 
		ConstantDataTask cdt = new ConstantDataTask();
		
		// I need to create the options here
		Map<String, Object> outputMap = new HashMap<String, Object>();

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

		Map <String, Object> optionMap = new HashMap<String, Object>();
		optionMap.put("layout", "Seaborn");
		
		// alignment={ggplot=[], selectors=[MovieBudget, RevenueDomestic]}}
		//   {0={layout=GGPlot, alignment={ggplot=[], selectors=[MovieBudget, RevenueDomestic]}}}
		Map<String, Object> alignmentMap = new Hashtable<String, Object>();
		alignmentMap.put("seaborn", new String[] {});
		alignmentMap.put("selectors", headers);
		optionMap.put("alignment", alignmentMap);
		// leaving out the alignment
		//optionMap.put(key, value)
		Map <String, Object> panelMap = new HashMap<String, Object>();

		int panelNum = 0;
		if(insight.getVarStore().containsKey(panelId))
			panelNum = (Integer)insight.getVarStore().get(panelId).getValue();
		panelNum++;
		insight.getVarStore().put(panelId, new NounMetadata(panelNum, PixelDataType.CONST_INT));

		panelMap.put(panelId+panelNum, optionMap);
		//panelMap.put(insight.getLastPanelId(), optionMap);
		//panelMap.put("2", optionMap);
		
		TaskOptions options = new TaskOptions(panelMap);
		
		cdt.setTaskOptions(options);
		Map<String, Object> formatMap = new Hashtable<String, Object>();
		formatMap.put("type", "TABLE");
		cdt.setFormatMap(formatMap);
		
		// replace the relplot
		command = command.replace("sns.relplot(", "");
		command = command.substring(0, command.length() - 1);
		
		bin = sw.toString();
		
		outputMap.put("headers", headers);
		outputMap.put("rawHeaders", headers);
		outputMap.put("values", new String[]{bin});
		outputMap.put("splot", command);	
		outputMap.put("format", format);
		
		// set the output so it can give it
		cdt.setOutputData(outputMap);

		// delete the pivot later
		if(bin.length() > 0) {
			Vector<NounMetadata> retList = new Vector<>();
			// can make panel id random moving forward
			InsightPanel daPanel = insight.getInsightPanel(panelId);
			//newWindow = daPanel != null;
			if(newWindow)
			{
				InsightPanel newPanel = new InsightPanel(panelId + panelNum, Insight.DEFAULT_SHEET_ID);
				this.insight.addNewInsightPanel(newPanel);
				NounMetadata noun = new NounMetadata(newPanel, PixelDataType.PANEL, PixelOperationType.PANEL_OPEN);
				retList.add(noun);
				retList.add( new NounMetadata(cdt, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA, PixelOperationType.FILE));
			
				if(newFrameVar != null)
					retList.add(newFrameVar);

				return new NounMetadata(retList, PixelDataType.VECTOR, PixelOperationType.VECTOR);
			}
			//return //new NounMetadata(cdt, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA, PixelOperationType.FILE);
		} 
		else
		{
			List<NounMetadata> outputs = new Vector<NounMetadata>(1);
			outputs.add(new NounMetadata(seabornFile, PixelDataType.CONST_STRING));
			return new NounMetadata(outputs, PixelDataType.CODE, PixelOperationType.CODE_EXECUTION);
		}
		return null;
	}
	
	@Override
	public String getExecutedCode() {
		return this.code;
	}

	@Override
	public LANGUAGE getLanguage() {
		return LANGUAGE.PYTHON;
	}

}
