package prerna.reactor.portals;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Locale;
import java.util.Properties;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateExceptionHandler;
import freemarker.template.Version;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.CmdExecUtil;
import prerna.util.Utility;


//MakePortal("b7ef29ce-92b3-4720-aece-f626ac48c424", "I3P", "Army")
public class MakePortalReactor extends AbstractReactor {
	
	private static final String PORTAL_NAME = "portalName";
	private static final String CLIENT = "client";
	private static final String VERSION = "version";
	private static final String ARCHETYPE = "archetype";
	private static final String  RECONF = "reconf"; // specifies whether to reconfig only or create a new portal from scratch

	
	// Inputs - portalName - Artifact ID, Client name - Group ID, Version (0.0.1 Default) - Version not given, Archetype (Simple Portal Default) - If not given
	
	
	public MakePortalReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey(), PORTAL_NAME, CLIENT, VERSION, ARCHETYPE, RECONF };
		this.keyRequired = new int[] {1,1,1,0,0,0};
	}

	@Override
	public NounMetadata execute() {

		// pulls the template from github.com/semoss/{Archetype}.git
		// creates a portal name folder in the app_asset/version
		// writes the details into a properties file
		// replaces the pom with the specified artifactId etc. 
		// Prints out it is ready to start building source code
		organizeKeys();

		String projectId = keyValue.get(keysToGet[0]);

		String portalName = keyValue.get(keysToGet[1]);
		String client = keyValue.get(keysToGet[2]);
		
		String version = "0.0.1";
		if(keyValue.containsKey(keysToGet[3]))
			version = keyValue.get(keysToGet[3]);
		
		String archetype = "https://github.com/prabhuk12/JART.git";
		if(keyValue.containsKey(keysToGet[4]))
			archetype = keyValue.get(keysToGet[4]);
		
		// will come to reconf shortly
		Properties projectValues = new Properties();
		projectValues.put(PORTAL_NAME, portalName);
		projectValues.put(CLIENT, client);
		projectValues.put(VERSION, version);
		
		projectValues.put(ReactorKeysEnum.PROJECT.getKey(), projectId);

		String projectFolder = AssetUtility.getProjectAssetFolder(projectId);
		
		try {
			pullGit(projectFolder, archetype, portalName);
			writePortalProperties(projectFolder + "/portals", projectValues, portalName);
			convertAllTemplates(projectFolder +"/portals", projectValues, portalName);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
			return NounMetadata.getErrorNounMessage(e.getLocalizedMessage());
		}
		
		return 
				NounMetadata.getSuccessNounMessage("Successfully created portal " + portalName);
	}
	
	
	// app/version/assets/portals
	private void pullGit(String projectFolder, String archetype, String portalName)
	{
		String mountName = Utility.getRandomString(5);
		
		String portalFolder = projectFolder + "/portals";
		
		File portalFolderFile = new File(portalFolder);
		if(!portalFolderFile.exists())
			portalFolderFile.mkdir();

		CmdExecUtil util = new CmdExecUtil(mountName, portalFolder, this.insight.getUser().getSocketClient(true));
		
		if(!archetype.startsWith("http")) // this is our local repo pull from it
			archetype = "https://github.com/semoss/" + archetype;
		
		util.executeCommand("git clone " + archetype + " " + portalName);
		
		// this should complete the process of git
	}

	private void writePortalProperties(String projectFolder, Properties projectValues, String portalName) throws Exception
	{		
		File file = new File(projectFolder + "/" + portalName + "/portal.properties");
		PrintWriter pw = new PrintWriter(new FileWriter(file));
		
		projectValues.list(pw);
		pw.flush();
		pw.close();
	}

	private void convertAllTemplates(String projectFolder, Properties projectValues, String portalName) throws Exception
	{		
		String portalFolder = projectFolder + "/" + portalName;
		portalFolder = portalFolder.replace("\\\\", "/");

		// main pom
		String inputFile = "pom.xml";
		String outputFile = portalFolder + "/pom.xml";
		convertTemplateToProject(projectValues, portalName, portalFolder, inputFile, outputFile);
		
		
		// be pom
		String beFolder = portalFolder + "/be";
		inputFile = "pom.xml";
		outputFile = beFolder + "/pom.xml";
		convertTemplateToProject(projectValues, portalName, beFolder, inputFile, outputFile);
		
		// fe pom
		String feFolder = portalFolder + "/fe";
		inputFile = "pom.xml";
		outputFile = feFolder + "/pom.xml";
		convertTemplateToProject(projectValues, portalName, feFolder, inputFile, outputFile);

	}
	
	
	private void convertTemplateToProject(Properties projectValues, String archetype, String templateDir, String inputFileName, String outputFileName) throws Exception
	{

		// navigate to the archetype folder
        Configuration cfg = new Configuration();

        cfg.setIncompatibleImprovements(new Version(2, 3, 20));
        cfg.setDefaultEncoding("UTF-8");
        cfg.setLocale(Locale.US);
        cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
        cfg.setDirectoryForTemplateLoading(new File(templateDir));

        // there are three templates here - main pom followed by BE followed by FE
        Template t = cfg.getTemplate(inputFileName);
        Writer out = new StringWriter();
        t.process(projectValues, out);
        
        String outputString = out.toString();

        // open the pom file and write it back
		FileWriter outputWriter = new FileWriter(new File(outputFileName));

        outputWriter.write(outputString);
        
        outputWriter.flush();
        outputWriter.close();
                
		//deleterFile.delete();		
	}
}
