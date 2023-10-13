package prerna.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.maven.shared.invoker.DefaultInvocationRequest;
import org.apache.maven.shared.invoker.DefaultInvoker;
import org.apache.maven.shared.invoker.InvocationOutputHandler;
import org.apache.maven.shared.invoker.InvocationRequest;
import org.apache.maven.shared.invoker.InvocationResult;
import org.apache.maven.shared.invoker.Invoker;
import org.apache.maven.shared.invoker.MavenInvocationException;

import prerna.reactor.AbstractReactor;
import prerna.reactor.ReactorFactory;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;


public class MvnReactor extends AbstractReactor
{
	public MvnReactor() {
		// need repository
		// Oauth
		// File name
		// Content
		this.keysToGet = new String[]{ReactorKeysEnum.MVN_GOALS.getKey()};
		this.keyRequired = new int[] {0};
	}

	
	
	public NounMetadata execute()
	{
		organizeKeys();
		// try to find if the system property is set or RDF_MAP has it
		// JAVA_HOME
		// MVN_HOME
		// if not throw error
		String javaHome = System.getProperty(Settings.JAVA_HOME);
		if(javaHome == null)
			javaHome = DIHelper.getInstance().getProperty(Settings.JAVA_HOME);
		if(javaHome == null)
			return NounMetadata.getErrorNounMessage("JAVA_HOME is not set, set as environment / RDF_MAP", null);		
		System.setProperty("JAVA_HOME",  javaHome);

		
		String mvnHome = System.getProperty(Settings.MVN_HOME);
		if(mvnHome == null)
			mvnHome = DIHelper.getInstance().getProperty(Settings.MVN_HOME);
		if(mvnHome == null)
			return NounMetadata.getErrorNounMessage("MVN_HOME is not set, set as environment / RDF_MAP", null);

		String workingDir = this.insight.getCmdUtil().getWorkingDir();
		
		// classes dir
		String classesDir = workingDir.substring(0, workingDir.indexOf("app_root") + "app_root".length()) + File.separator + "target";
		
		InvocationRequest request = new DefaultInvocationRequest();
        request.setMavenOpts("-DclassesDir=" + classesDir);

		InvocationOutputHandler outputHandler = new InvocationOutputHandler(){
			
		File file = new File(workingDir + File.separator + "temp.mvn.output");
		FileWriter fw = null;
		
        @Override
        public void consumeLine(String line) throws IOException {

        	if(fw == null)
        	{
        		System.err.println("File writer is null ");
        		fw = new FileWriter(file);
        	}
        	fw.write(line + "\n");
        	fw.flush();
        	
            if (line.contains("<libertySettingsFolder> must be a directory")) {
                throw new IOException("Caught expected MojoExecutionException - " + line);
            }
        }
	    };
		Invoker invoker = new DefaultInvoker();		
		invoker.setOutputHandler(outputHandler);
		invoker.setMavenHome(new File(Utility.normalizePath(mvnHome)));
		
		// path to maven executable		
		String pomFileName = workingDir + File.separator + "pom.xml";
		File pomFile = new File(pomFileName);
		if(!pomFile.exists())
			return NounMetadata.getErrorNounMessage("Not a maven project, please navigate to the dir where you have pom.xml", null);
		request.setPomFile(pomFile);
		List <String> goals = new ArrayList();
		goals.add("clean");
		goals.add("compile");
		
		if(keyValue.containsKey(keysToGet[0]))
		{
			String inputGoals = (String)keyValue.get(keysToGet[0]);
			String [] goalTokens = inputGoals.split(" ");
			
			goals = Arrays.asList(goalTokens);
		}
		request.setGoals( goals);
				 
		try {
			InvocationResult result;
			result = invoker.execute( request );
			if ( result.getExitCode() != 0 )
			{
				String errorMessage = composeErrorMessage(new File(workingDir + File.separator + "temp.mvn.output"));
			    return new NounMetadata(errorMessage,PixelDataType.CONST_STRING);
			}		
		} catch (MavenInvocationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return new NounMetadata("Compile successful", PixelDataType.CONST_STRING);
		 
	}	
	
	
	public String composeErrorMessage(File file)
	{
		StringBuffer output = new StringBuffer();
		try (BufferedReader br = new BufferedReader(new FileReader(file))){
			
			String data = null;
			
			while((data = br.readLine() ) != null)
				output.append(data).append("\n");
			
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return output.toString();
	}
	
}