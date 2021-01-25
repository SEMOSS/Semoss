package prerna.io.connector.usajobs;

import java.util.List;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.AssetUtility;
import prerna.util.Utility;

public class USAJobsByStateDownload extends AbstractReactor {

	public USAJobsByStateDownload() {
		this.keysToGet = new String[] { "cred", "state", ReactorKeysEnum.FILE_NAME.getKey()};
	}

	public NounMetadata execute() {
		organizeKeys();
		
		// this would be a vector
		List <String> cred = (List)this.store.getNoun("cred").getAllValues();
		List <String> state = (List)this.store.getNoun("state").getAllValues();
		
		String fileName = null;
		if(keyValue.containsKey(keysToGet[2]))
			fileName = keyValue.get(keysToGet[2]);
		else
			fileName = Utility.getRandomString(5) + ".csv";
		
		
		// should we put thi in temp
		fileName  = this.insight.getInsightFolder() + java.nio.file.FileSystems.getDefault().getSeparator() + fileName;

		UsaJobsUtil u = new UsaJobsUtil(fileName);
		u.runStateSearch(cred, state);
		
		// set the file for download
		String exportName = "USA_JOBS" + UsaJobsUtil.getToday("_");
		this.insight.addExportFile(exportName, fileName);
		NounMetadata retNoun = new NounMetadata(exportName, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD); 
		return retNoun;
	}
}
