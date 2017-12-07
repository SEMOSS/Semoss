package prerna.sablecc2.reactor.git;

import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.log4j.Logger;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.DIHelper;
import prerna.util.GitHelper;
import prerna.util.MosfitSyncHelper;

public class IsGit extends AbstractReactor {

	public IsGit()
	{
		this.keysToGet = new String[]{"app"};
	}
	
	@Override
	public NounMetadata execute() {
		// TODO Auto-generated method stub
		Logger logger = getLogger(this.getClass().getName());
		try {
			organizeKeys();
			
			logger.info("Checking - Please wait");
			String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
			//helper.synchronize(baseFolder + "/db/Mv2Git4", "Mv4", userName, password, true);

			String dbName = baseFolder + "/db/" + keyValue.get(keysToGet[0]);	

			boolean isGit = GitHelper.isGit(dbName);
			// if nothing is sent it means it is dual
			logger.info("Complete");

			return new NounMetadata(isGit, PixelDataType.BOOLEAN);
		} catch (Exception e) {
			e.printStackTrace();
			logger.fatal(e.getMessage());
		}
		return null;
	}
	
	/**
	 * Need to get the appropriate files to perform indexing!
	 * @param filesChanged
	 * @return
	 */
	private Map<String, List<String>> getMosfetFiles(Map <String, List<String>> filesChanged) {
		Map<String, List<String>> mosfetFiles = new Hashtable<String, List<String>>();
		if(filesChanged.containsKey("ADD")) {
			List<String> files = getMosfetFiles(filesChanged.get("ADD"));
			if(!files.isEmpty()) {
				mosfetFiles.put("ADD", files);
			}
		}
		
		if(filesChanged.containsKey("MOD")) {
			List<String> files = getMosfetFiles(filesChanged.get("ADD"));
			if(!files.isEmpty()) {
				mosfetFiles.put("ADD", files);
			}		
		}
		
		if(filesChanged.containsKey("REN")) {
			List<String> files = getMosfetFiles(filesChanged.get("ADD"));
			if(!files.isEmpty()) {
				mosfetFiles.put("ADD", files);
			}		
		}
		
		if(filesChanged.containsKey("DEL")) {
			List<String> files = getMosfetFiles(filesChanged.get("ADD"));
			if(!files.isEmpty()) {
				mosfetFiles.put("ADD", files);
			}		
		}
		return mosfetFiles;
	}
	
	private List<String> getMosfetFiles(List<String> potentialFiles) {
		List<String> mosfetFiles = new Vector<String>();
		if(potentialFiles != null) {
			for(String f : potentialFiles) {
				if(f.endsWith(".mosfet")) {
					mosfetFiles.add(f);
				}
			}
		}
		return mosfetFiles;
	}

}
