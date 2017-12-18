package prerna.sablecc2.reactor.git;

import org.apache.log4j.Logger;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.git.GitCollaboratorUtils;

public class RemoveCollaborator extends AbstractReactor {

	public RemoveCollaborator() {
		this.keysToGet = new String[]{"repository", "collaborator","username", "password"};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();

		Logger logger = getLogger(this.getClass().getName());
		logger.info("Establishing connection ");
		logger.info("This can take several minutes depending on the speed of your internet ");

		String repository = this.keyValue.get(this.keysToGet[0]);
		String collaborator = this.keyValue.get(this.keysToGet[1]);
		String username = this.keyValue.get(this.keysToGet[2]);
		String password = this.keyValue.get(this.keysToGet[3]);
				
		logger.info("Removing Collaborator...");
		GitCollaboratorUtils.removeCollaborator(repository, username, password, collaborator);
		logger.info("Collaborator Removed");
		return new NounMetadata(true, PixelDataType.CONST_STRING, PixelOperationType.MARKET_PLACE);
	}

}
