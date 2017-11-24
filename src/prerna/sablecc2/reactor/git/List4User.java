package prerna.sablecc2.reactor.git;

import java.util.Vector;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.GitHelper;

public class List4User extends AbstractReactor {

	// clone from remote
	// this assumes the local directory is not there
	
	
	public List4User()
	{
		this.keysToGet = new String[]{"username", "password"};
	}
	
	@Override
	public NounMetadata execute() {
		// TODO Auto-generated method stub
		// creates a remote repository
		organizeKeys();
		GitHelper helper = new GitHelper();
		String password = keyValue.get(keysToGet[1]);
		password = password.equalsIgnoreCase("null") ? null: password;

		Vector <String> repoList = helper.listRemotes(keyValue.get(keysToGet[0]), password);
		
		return new NounMetadata(repoList, PixelDataType.VECTOR);
	}

}
