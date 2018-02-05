package prerna.util.git.reactors;

import org.apache.log4j.Logger;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.git.GitCollaboratorUtils;

public class AddAppCollaborator extends AbstractReactor {

	public AddAppCollaborator() {
		this.keysToGet = new String[]{
				ReactorKeysEnum.REPOSITORY.getKey(), ReactorKeysEnum.COLLABORATOR.getKey(), 
				ReactorKeysEnum.USERNAME.getKey(), ReactorKeysEnum.PASSWORD.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		Logger logger = getLogger(this.getClass().getName());
		logger.info("Establishing connection ");
		
		String repo = this.keyValue.get(this.keysToGet[0]);
		String username = this.keyValue.get(this.keysToGet[2]);
		String password = this.keyValue.get(this.keysToGet[3]);
		String collaborator = this.keyValue.get(this.keysToGet[1]);

		logger.info("Adding Collaborator = " + collaborator);
		logger.info("This can take several minutes depending on the speed of your internet ");
		GitCollaboratorUtils.addCollaborator(repo, username, password, collaborator);
		logger.info("Collaborator Added");
		return new NounMetadata(true, PixelDataType.CONST_STRING, PixelOperationType.MARKET_PLACE);
	}

}
