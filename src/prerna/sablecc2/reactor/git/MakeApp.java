package prerna.sablecc2.reactor.git;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.DIHelper;
import prerna.util.GitHelper;

public class MakeApp extends AbstractReactor {

	// clone from remote
	// this assumes the local directory is not there
	
	public MakeApp()
	{
		super.keysToGet = new String[]{"app", "remote", "username", "password", "smss", "type"};
	}
	
	@Override
	public NounMetadata execute() {
		// TODO Auto-generated method stub
		// creates a remote repository
		organizeKeys();
		GitHelper helper = new GitHelper();
		
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		String smss = keyValue.get(keysToGet[4]);
		
		// later pass SMSS
		smss = smss.contains("null")?null:smss;

		helper.makeRemoteFromApp(baseFolder, keyValue.get(keysToGet[0]), keyValue.get(keysToGet[1]), true, keyValue.get(keysToGet[2]), keyValue.get(keysToGet[3]));
		
		return new NounMetadata("Success", PixelDataType.CONST_STRING);
	}

}
