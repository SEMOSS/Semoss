package prerna.util.git.reactors;

import org.apache.log4j.Logger;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.git.GitCollaboratorUtils;

public class RemoveAppCollaborator extends GitBaseReactor {

	public RemoveAppCollaborator() {
		this.keysToGet = new String[]{
				ReactorKeysEnum.REPOSITORY.getKey(), ReactorKeysEnum.COLLABORATOR.getKey(), 
				ReactorKeysEnum.USERNAME.getKey(), ReactorKeysEnum.PASSWORD.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();

		Logger logger = getLogger(this.getClass().getName());
		logger.info("Establishing connection ");
		logger.info("This can take several minutes depending on the speed of your internet ");

		String repository = this.keyValue.get(this.keysToGet[0]);
		String collaborator = this.keyValue.get(this.keysToGet[1]);
		logger.info("Removing Collaborator...");
		if(keyValue.size() == 4)
		{
			String username = this.keyValue.get(this.keysToGet[2]);
			String password = this.keyValue.get(this.keysToGet[3]);
			GitCollaboratorUtils.removeCollaborator(repository, username, password, collaborator);
		}
		else
		{
			String token = getToken();
			GitCollaboratorUtils.removeCollaborator(repository, collaborator, token);
		}
		logger.info("Collaborator Removed");
		return new NounMetadata(true, PixelDataType.CONST_STRING, PixelOperationType.MARKET_PLACE);
	}

}
