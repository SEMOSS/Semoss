package prerna.sablecc2.reactor.git;

import java.util.Hashtable;
import java.util.List;

import org.apache.log4j.Logger;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.DIHelper;
import prerna.util.GitHelper;

public class Sync extends AbstractReactor {

	
	public Sync()
	{
		this.keysToGet = new String[]{"app", "remoteApp", "username", "password", "dual"};
	}
	
	@Override
	public NounMetadata execute() {
		// TODO Auto-generated method stub
		Logger logger = getLogger(this.getClass().getName());
		try {
			organizeKeys();
			
			GitHelper helper = new GitHelper();
			String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
			//helper.synchronize(baseFolder + "/db/Mv2Git4", "Mv4", userName, password, true);

			String dbName = baseFolder + "/db/" + keyValue.get(keysToGet[0]);	
			
			// if nothing is sent it means it is dual
			boolean dual = !keyValue.containsKey(keysToGet[4]);
			
			// if it is null or true dont worry

			Hashtable <String, List<String>> filesChanged = helper.synchronize(dbName, keyValue.get(keysToGet[1]), keyValue.get(keysToGet[2]), keyValue.get(keysToGet[3]), dual);
			
			StringBuffer output = new StringBuffer("SUCCESS \n");
			output.append("ADDED : ");
			if(filesChanged.containsKey("ADD"))
				output.append(filesChanged.get("ADD").size());
			else
				output.append("0");
			output.append(" , MODIFIED : ");
			if(filesChanged.containsKey("MOD"))
				output.append(filesChanged.get("MOD").size());
			else
				output.append("0");
			output.append(" , RENAMED : ");
			if(filesChanged.containsKey("REN"))
				output.append(filesChanged.get("REN").size());
			else
				output.append("0");
			output.append(" , DELETED : ");
			if(filesChanged.containsKey("DEL"))
				output.append(filesChanged.get("DEL").size());
			else
				output.append("0");

			// need something to add these files to  SOLR
			// Process to Solr (filesChanged)
			// removing the files or reindexing the files etc. 

			
			return new NounMetadata(output.toString(), PixelDataType.CONST_STRING);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.fatal(e.getMessage());

		}
		return null;
	}

}
