package prerna.sablecc2.reactor.project;

import java.io.File;
import java.io.IOException;

import net.snowflake.client.jdbc.internal.apache.commons.io.FileUtils;
import prerna.project.api.IProject;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.ProjectCustomReactorCompilator;
import prerna.util.Utility;

public class CompileReactor extends AbstractReactor {

	public CompileReactor()
	{
		// I dont know if we should allow insight anymore.. 
		this.keysToGet = new String[] {ReactorKeysEnum.PROJECT.getKey()}; //, ReactorKeysEnum.INSIGHT_NAME.getKey()};
		
	}
	
	@Override
	public NounMetadata execute() {
		// TODO Auto-generated method stub
		organizeKeys();
		
		String projectId = keyValue.get(keysToGet[0]);
		if(projectId == null)
			// try with current insights
			projectId = this.insight.getProjectId();
		
		if(projectId == null)
		{
			return NounMetadata.getErrorNounMessage("No project provided for compilation");
		}
		
		StringBuilder output = new StringBuilder("");
		
		if(projectId != null)
		{
			IProject project = Utility.getProject(projectId);
			ProjectCustomReactorCompilator.reset(projectId);
			project.compileReactors(null);
			
			try {
				// get the compiler output
				String compilerOutput = project.getProjectAssetFolder() + "/classes/compileerror.out";
				output.append("Project - ").append(project.getProjectName()).append("\n");
				output.append("-------------").append("\n");
				String compileOutput = FileUtils.readFileToString(new File(compilerOutput));
				if(compileOutput.length() > 0)
					output.append(compileOutput);
				else
					output = new StringBuilder("No Errors");
				return new NounMetadata(output + "", PixelDataType.CONST_STRING);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
		return null;
	}

}
