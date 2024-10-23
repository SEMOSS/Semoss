package prerna.reactor;

import java.io.File;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IStorageEngine;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.engine.impl.vector.AbstractVectorDatabaseEngine;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class SyncVectorDatabaseFromStorage extends AbstractReactor{
	
	private static final Logger classLogger = LogManager.getLogger(SyncVectorDatabaseFromStorage.class);
	
	private static final String STORAGE_ENGINE_ID = "storageEngineId";	
	private static final String NAME = "Name";
	private static final String PATH = "Path";
	private static final String MODIFIED_TIME = "ModTime";
	private static final String FILE_NAME = "fileName";
	
	
	public SyncVectorDatabaseFromStorage() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.ENGINE.getKey(),
				STORAGE_ENGINE_ID, ReactorKeysEnum.STORAGE_PATH.getKey()
		};
		this.keyRequired = new int[] {1, 1, 1};
	}	

	@Override
	public NounMetadata execute() {
		organizeKeys();			
		
		boolean output = false;
		List<String> engineList = new ArrayList<String>(); 
		List<String> fileList = new ArrayList<String>(); 
		List<String> uploadedFiles = new ArrayList<String>();
		List<String> vectorDbFiles = new ArrayList<String>();
		
		GenRowStruct grs = this.store.getNoun(this.keysToGet[0]);
		if (grs != null && !grs.isEmpty()) {
			int size = grs.size();
			for (int i = 0; i < size; i++) {
				engineList.add(grs.get(i).toString());
			}
		}
		String storageId = this.keyValue.get(this.keysToGet[1]);
		String storagePath = this.keyValue.get(this.keysToGet[2]);
		
		try {
			IStorageEngine storage = Utility.getStorage(storageId);
			List<Map<String, Object>> storageFileList = storage.listDetails(storagePath);
			for (Map<String, Object> storageFile : storageFileList) { 
	        	if(storageFile.get(PATH).equals(storageFile.get(NAME))) {
	        		uploadedFiles.add(storageFile.get(NAME).toString());
	        	}
			}	
			
			fileList = fetchRecentUploadsFromStorage(insight,storage, storageFileList); 
			
			for(String engineId : engineList) {
				if(!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), engineId)) {
					throw new IllegalArgumentException("Vector db " + engineId + " does not exist or user does not have access to this engine");
				}
	
				IVectorDatabaseEngine vectorDatabase = Utility.getVectorDatabase(engineId);
				if (vectorDatabase == null) {
					throw new SemossPixelException("Unable to find engine");
				}
				
				Map<String, Object> paramMap = getMap();
				if(paramMap == null) {
					paramMap = new HashMap<String, Object>();
				}
				
				paramMap.put(AbstractVectorDatabaseEngine.INSIGHT, this.insight);				    
		       
		        List<Map<String, Object>> documentList = vectorDatabase.listDocuments(paramMap);
		        for (Map<String, Object> document : documentList) { 
		        	if(!uploadedFiles.contains(document.get(FILE_NAME).toString())){
		        		vectorDbFiles.add(document.get(FILE_NAME).toString());
		        	}
		        }
		        
		        if(!vectorDbFiles.isEmpty()) {
		        	vectorDatabase.removeDocument(vectorDbFiles, paramMap);
		        }
		        if(!fileList.isEmpty()) {
		        	vectorDatabase.addDocument(fileList, paramMap);
		        }
		        output = true;			
			}			
			 
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error occurred downloading storage file to vector");
		}
		
		return new NounMetadata(output, PixelDataType.BOOLEAN, PixelOperationType.OPERATION);
		
	}	
	
	public List<String> fetchRecentUploadsFromStorage(Insight insight,IStorageEngine storage, List<Map<String, Object>> storageFileList) {
		File outputFile = null;
		List<String> fileList = new ArrayList<String>(); 
		
		try {			
			String insightId = insight.getInsightId();
			Insight in = InsightStore.getInstance().get(insightId);		
			Insight newInsight = new Insight();
			if(in == null) {				
				InsightStore.getInstance().put(newInsight);				
				in = InsightStore.getInstance().get(newInsight.getInsightId());
			}
			File storageFileFolderDir = new File( Utility.normalizePath(in.getInsightFolder()));
			
			Date lastChecked = getLastCheckedDate();			
			
			for (Map<String, Object> storageFile : storageFileList) { 
	        	if(storageFile.get(PATH).equals(storageFile.get(NAME))) {
	        		Instant instant = Instant.parse(storageFile.get(MODIFIED_TIME).toString());  
	        		Date fileModifiedDate = Date.from(instant);
	        		if(fileModifiedDate.after(lastChecked)) {	        			
	        			storage.copyToLocal(this.keyValue.get(this.keysToGet[2]) + DIR_SEPARATOR + storageFile.get(NAME).toString(), 
	        					storageFileFolderDir.toString());
	        			
	        			outputFile = new File(storageFileFolderDir + DIR_SEPARATOR + storageFile.get(NAME).toString());
	        			fileList.add(outputFile.toString());
	        		}
	        	}
	        }
		}catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error occurred downloading files from storage.");
		}
		
		return fileList;
	}	
	
	private Date getLastCheckedDate() {
		 Calendar calendar = Calendar.getInstance();
		 
		 calendar.add(Calendar.DAY_OF_MONTH, -1);
		 calendar.set(Calendar.HOUR_OF_DAY, 0);
		 calendar.set(Calendar.MINUTE, 0);
		 calendar.set(Calendar.SECOND, 0);
		 calendar.set(Calendar.MILLISECOND,0);  
		 
	     return calendar.getTime();
	}
	
	private Map<String, Object> getMap() {
        GenRowStruct mapGrs = this.store.getNoun(keysToGet[1]);
        if(mapGrs != null && !mapGrs.isEmpty()) {
            List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
            if(mapInputs != null && !mapInputs.isEmpty()) {
                return (Map<String, Object>) mapInputs.get(0).getValue();
            }
        }
        List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
        if(mapInputs != null && !mapInputs.isEmpty()) {
            return (Map<String, Object>) mapInputs.get(0).getValue();
        }
        return null;
    }	
	
}
