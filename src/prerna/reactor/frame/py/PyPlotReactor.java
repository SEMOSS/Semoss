package prerna.reactor.frame.py;

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
import prerna.ds.py.PyTranslator;
import prerna.ds.py.PyUtils;
import prerna.om.Insight;
import prerna.om.InsightPanel;
import prerna.om.Variable.LANGUAGE;
import prerna.query.interpreters.PandasInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
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
import prerna.util.insight.InsightUtility;

public class PyPlotReactor extends AbstractPyFrameReactor implements ICodeExecution {
	
	private static final String CLASS_NAME = PyPlotReactor.class.getName();
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
		
		//check if py terminal is disabled
		String disable_py_terminal =  DIHelper.getInstance().getProperty(Constants.DISABLE_PY_TERMINAL);
		if(disable_py_terminal != null && !disable_py_terminal.isEmpty() ) {
			 if(Boolean.parseBoolean(disable_py_terminal)) {
					throw new IllegalArgumentException("Python terminal has been disabled.");
			 }
		}
		Logger logger = getLogger(CLASS_NAME);

		this.code = Utility.decodeURIComponent(this.curRow.get(0).toString());
		int tokens = code.split("\\n").length;

		PyTranslator pyTranslator = this.insight.getPyTranslator();
		pyTranslator.setLogger(logger);
		return handlePyPlot(code);		
	}
	
	// handles all sorts of plot
	// seaborn
	// pyplot etc. 
	public NounMetadata handlePyPlot(String command) {
		//organizeKeys();
		
		String panelId = "new_seaborn_panel";

		PyTranslator pyt = this.insight.getPyTranslator();
		pyt.setLogger(this.getLogger(this.getClass().getName()));
		
		String format = "png";
		boolean newWindow = true;
		
		// need something here to adjust the types
		// need to move this to utilities 
		// will move it once we have figured it out
		
		// there is a third scenario that needs to be addressed i..e when the frame type is R
		
		ITableDataFrame thisFrame = insight.getCurFrame();
		String type = thisFrame.getFrameType().getTypeAsString();
		
		NounMetadata newFrameVar = null;
		if(thisFrame == null) {
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
			
			thisFrame = insight.getCurFrame();
		}
		else if(thisFrame != null && type.equalsIgnoreCase("R"))
		{
			// need to do the synchronization routine
			
		}
		
		SelectQueryStruct qs = InsightUtility.getFilteredQsForFrame(thisFrame, null);
		
		
		qs.getRelations().clear();
		
		qs = QSAliasToPhysicalConverter.getPhysicalQs(qs, thisFrame.getMetaData());

		PandasInterpreter interp = new PandasInterpreter();
		interp.setDataTableName(thisFrame.getName(), thisFrame.getName() + "w" + ".cache['data']");
		interp.setDataTypeMap(thisFrame.getMetaData().getHeaderToTypeMap());
		interp.setQueryStruct(qs);
		interp.setKeyCache(new ArrayList());
		
		String frameName = thisFrame.getName();
		String assigner = "";
		
		String selectorQuery = interp.composeQuery();
		// remove the splitter
		selectorQuery = selectorQuery.replace(".to_dict('split')", "");
		
		String subDataTable = "pd.DataFrame(" + selectorQuery +")"; // + "['data'])";

		
		command = command.replaceAll("\\s", ""); // remove all spaces
		assigner = "plotterframe = " + subDataTable;
		if(command.contains("data=" + thisFrame.getName() + ""))
		{
			command = command.replace("data=" + thisFrame.getName() + "", "data=plotterframe");
		}
		
		String ROOT = insight.getInsightFolder();
		ROOT = ROOT.replace("\\", "/");


		String importPyPlot = "import matplotlib.pyplot as plt";
		String importSeaborn = "import seaborn as sns";
		String clearPlot = "plt.clf()";

		String runPlot = command;
		String seabornFile = Utility.getRandomString(6);
		String printFile = "print(saveFile)";
		String saveFileName = "saveFile = '" + ROOT + "/" + seabornFile + "." + format + "'";
		String savePlot = "plt.savefig(saveFile)";
		String removeSeaborn = "del(sns)";
		String removeSaveFile = "del(saveFile)";
		
		seabornFile = (String)pyt.runPyAndReturnOutput(assigner, importPyPlot, importSeaborn, clearPlot, saveFileName, runPlot, savePlot, removeSeaborn, printFile, removeSaveFile);

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
		String [] headers = thisFrame.getColumnHeaders();

		
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
		//command = command.replace("sns.relplot(", "");
		//command = command.substring(0, command.length() - 1);
		
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

	@Override
	public boolean isUserScript() {
		return true;
	}
}
