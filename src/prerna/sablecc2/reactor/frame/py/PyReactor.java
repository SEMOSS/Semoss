package prerna.sablecc2.reactor.frame.py;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.util.Base64;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.ds.py.PyTranslator;
import prerna.ds.py.PyUtils;
import prerna.om.Insight;
import prerna.om.InsightPanel;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ConstantDataTask;
import prerna.sablecc2.om.task.options.TaskOptions;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Utility;

public class PyReactor extends AbstractReactor {
	
	private static transient SecurityManager defaultManager = System.getSecurityManager();
	private static final String CLASS_NAME = PyReactor.class.getName();
	
	@Override
	public NounMetadata execute() {
		if(!PyUtils.pyEnabled()) {
			throw new IllegalArgumentException("Python is not enabled to use the following command");
		}
		Logger logger = getLogger(CLASS_NAME);

		String code = Utility.decodeURIComponent(this.curRow.get(0).toString());

		PyTranslator pyTranslator = this.insight.getPyTranslator();
		pyTranslator.setLogger(logger);
		//String output = pyTranslator.runPyAndReturnOutput(code);
		String output = null;
		
		if(code.startsWith("sns."))
			return handleSeaborn(code);
		
		if(AbstractSecurityUtils.securityEnabled()) {
			output = pyTranslator.runPyAndReturnOutput(insight.getUser().getAppMap(), code) + "";
		} else {
			output = pyTranslator.runPyAndReturnOutput(code) + "";
		}
		List<NounMetadata> outputs = new Vector<NounMetadata>(1);
		outputs.add(new NounMetadata(output, PixelDataType.CONST_STRING));
		return new NounMetadata(outputs, PixelDataType.CODE, PixelOperationType.CODE_EXECUTION);
	}
	
	public NounMetadata handleSeaborn(String command)
	{
		//organizeKeys();

		PyTranslator pyt = this.insight.getPyTranslator();
		pyt.setLogger(this.getLogger(this.getClass().getName()));
		
		String format = "png";
		
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
		
		String importSeaborn = "import seaborn as sns";
		String runPlot = "daplot = " + command;
		String seabornFile = Utility.getRandomString(6);
		String printFile = "print(saveFile)";
		String saveFileName = "saveFile = ROOT + '/" + seabornFile + "." + format + "'";
		String savePlot = "daplot.savefig(saveFile)";
		String removeSeaborn = "del(sns)";
		String removeSaveFile = "del(saveFile)";
		
		seabornFile = (String)pyt.runPyAndReturnOutput(importSeaborn, saveFileName, runPlot, savePlot, removeSeaborn, printFile, removeSaveFile);

		// get the insight folder
		String IF = insight.getInsightFolder();
		seabornFile = seabornFile.replace("$IF", IF);
		
		StringWriter sw = new StringWriter();
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
		
		// Need to figure out if I am trying to delete the image and URI encode it at some point.. 
		ConstantDataTask cdt = new ConstantDataTask();
		
		// I need to create the options here
		Map<String, Object> outputMap = new HashMap<String, Object>();

		// need to do all the sets
		cdt.setFormat("TABLE");
		
		Map <String, Object> optionMap = new HashMap<String, Object>();
		optionMap.put("layout", "Seaborn");
		
		// alignment={ggplot=[], selectors=[MovieBudget, RevenueDomestic]}}
		Map<String,Object> alignmentMap = new HashMap<String, Object>();
		alignmentMap.put("Seaborn", new String[] {});
		alignmentMap.put("selector",new String[] {"a", "b"});
		optionMap.put("alignment", alignmentMap);
		
		// leaving out the alignment
		//optionMap.put(key, value)
		Map <String, Object> panelMap = new HashMap<String, Object>();
		panelMap.put(insight.getLastPanelId(), optionMap);
		//panelMap.put("2", optionMap);
		
		TaskOptions options = new TaskOptions(panelMap);
		
		cdt.setTaskOptions(options);
		//cdt.setHeaderInfo(task.getHeaderInfo());
		//cdt.setSortInfo(task.getSortInfo());
		//cdt.setId(task.getId());
		Map<String, Object> formatMap = new Hashtable<String, Object>();
		formatMap.put("type", "TABLE");
		cdt.setFormatMap(formatMap);
		
		// replace the relplot
		command = command.replace("sns.relplot(", "");
		command = command.substring(0, command.length() - 1);
		
		String bin = sw.toString();
		
		outputMap.put("headers", new String[] {});
		outputMap.put("rawHeaders", new String[] {});
		outputMap.put("values", new String[]{bin});
		outputMap.put("splot", command);	
		outputMap.put("format", format);
		
		// set the output so it can give it
		cdt.setOutputData(outputMap);

		// delete the pivot later
		if(bin.length() != 0) {
			Vector<NounMetadata> retList = new Vector<>();
			// can make panel id random moving forward
			InsightPanel newPanel = new InsightPanel("999", Insight.DEFAULT_SHEET_ID);
			this.insight.addNewInsightPanel(newPanel);
			NounMetadata noun = new NounMetadata(newPanel, PixelDataType.PANEL, PixelOperationType.PANEL_OPEN);
			//retList.add(noun);

			retList.add( new NounMetadata(cdt, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA, PixelOperationType.FILE));
			
			//return new NounMetadata(retList, PixelDataType.VECTOR, PixelOperationType.VECTOR);
			return new NounMetadata(cdt, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA, PixelOperationType.FILE);
		} else
		{
			List<NounMetadata> outputs = new Vector<NounMetadata>(1);
			outputs.add(new NounMetadata(seabornFile, PixelDataType.CONST_STRING));
			return new NounMetadata(outputs, PixelDataType.CODE, PixelOperationType.CODE_EXECUTION);
		}
	}
	

}
