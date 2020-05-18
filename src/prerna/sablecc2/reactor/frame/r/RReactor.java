package prerna.sablecc2.reactor.frame.r;

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
import prerna.sablecc.ReactorSecurityManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ConstantDataTask;
import prerna.sablecc2.om.task.options.TaskOptions;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.util.Utility;

public final class RReactor extends AbstractReactor {
	
	private static transient SecurityManager defaultManager = System.getSecurityManager();
	private static final String CLASS_NAME = RReactor.class.getName();
	
	@Override
	public NounMetadata execute() {
		ReactorSecurityManager tempManager = new ReactorSecurityManager();
		tempManager.addClass(CLASS_NAME);
		System.err.println(".");
		System.setSecurityManager(tempManager);
		
		Logger logger = getLogger(CLASS_NAME);
		AbstractRJavaTranslator rJavaTranslator = this.insight.getRJavaTranslator(logger);
		rJavaTranslator.startR();
		
		
		String code = Utility.decodeURIComponent(this.curRow.get(0).toString());
		logger.info("Execution r script: " + code);
		
		if(code.startsWith("ggplot"))
			return handleGGPlot(code);
		
		String output = null;
		if(AbstractSecurityUtils.securityEnabled()) {
			output = rJavaTranslator.runRAndReturnOutput(code, insight.getUser().getAppMap());
		} else {
			output = rJavaTranslator.runRAndReturnOutput(code);
		}
		List<NounMetadata> outputs = new Vector<>(1);
		outputs.add(new NounMetadata(output, PixelDataType.CONST_STRING));
		
		// set back the original security manager
		tempManager.removeClass(CLASS_NAME);
		System.setSecurityManager(defaultManager);	
		
		return new NounMetadata(outputs, PixelDataType.CODE, PixelOperationType.CODE_EXECUTION);
	}
	
	public NounMetadata handleGGPlot(String ggplotCommand)
	{
		// I need to see how to get this to temp
		String fileName = Utility.getRandomString(6);
		String dir = insight.getUserFolder() + "/Temp";
		dir = dir.replaceAll("\\\\", "/");
		File tempDir = new File(dir);
		if(!tempDir.exists())
			tempDir.mkdir();

		StringBuilder ggplotter = new StringBuilder("{library(\"ggplot2\");"); // library(\"RCurl\");");
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

		ggplotter.append(ggsaveFile + " <- " + "paste(ROOT,\"/" + ggsaveFile + "." +format +"\", sep=\"\"); ");

		if(!animate)
			ggplotter.append("ggsave(" + ggsaveFile + ");");
		else
			ggplotter.append("anim_save(" + ggsaveFile + ", " + plotString + ");");
		
		ggplotter.append("print(" + ggsaveFile + ")");

		ggplotter.append("}");

		Logger logger = getLogger(CLASS_NAME);
		AbstractRJavaTranslator rJavaTranslator = this.insight.getRJavaTranslator(logger);
		// run the ggplotter command
		String fName = rJavaTranslator.runRAndReturnOutput(ggplotter.toString());

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
		ConstantDataTask cdt = new ConstantDataTask();
		// need to do all the sets
		cdt.setFormat("TABLE");

		// TaskOptions options = new TaskOptions(optionMap);
		// need to do all the sets
		cdt.setFormat("TABLE");
		//cdt.setHeaderInfo(task.getHeaderInfo());
		//cdt.setSortInfo(task.getSortInfo());
		//cdt.setId(task.getId());
		Map<String, Object> formatMap = new Hashtable<String, Object>();
		formatMap.put("type", "TABLE");
		cdt.setFormatMap(formatMap);
		
		//   {0={layout=GGPlot, alignment={ggplot=[], selectors=[MovieBudget, RevenueDomestic]}}}

		Map <String, Object> optionMap = new HashMap<String, Object>();
		optionMap.put("layout", "GGPlot");

		// leaving out the alignment
		//optionMap.put(key, value)
		Map <String, Object> panelMap = new HashMap<String, Object>();
		panelMap.put(this.insight.getLastPanelId(), optionMap);
		
		TaskOptions options = new TaskOptions(panelMap);
		
		cdt.setTaskOptions(options);
		//cdt.setHeaderInfo();
		//cdt.setSortInfo(task.getSortInfo());
		//cdt.setId(task.getId());

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
		}

		if(animate)
			ggplotCommand = ggplotCommand + " + animate";

		ggplotCommand = ggplotCommand.replaceAll("\"", "\\\\\"");
		// remove the variable
		rJavaTranslator.runRAndReturnOutput(ggremove);

		outputMap.put("headers", new String[] {});
		outputMap.put("rawHeaders", new String[] {});
		outputMap.put("values", new String[]{sw.toString()});
		outputMap.put("ggplot", ggplotCommand);	
		outputMap.put("format", format);

		// set the output so it can give it
		cdt.setOutputData(outputMap);
		new File(retFile).delete();

		// delete the pivot later
		return new NounMetadata(cdt, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA, PixelOperationType.FILE);

	}
	
	

}
