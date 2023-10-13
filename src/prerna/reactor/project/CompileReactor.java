package prerna.reactor.project;

import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.reactor.ProjectCustomReactorCompilator;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
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
					
			// get the compiler output
			String compileOutput = project.getCompileOutput();
			output.append("Project - ").append(project.getProjectName()).append("\n");
			output.append("-------------").append("\n");
			if(compileOutput != null)
				output.append(compileOutput);
			else
				output = new StringBuilder("No Errors");
			return new NounMetadata(output + "", PixelDataType.CONST_STRING);
			
		}
		
		return null;
	}

}
