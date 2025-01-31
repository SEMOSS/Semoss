package prerna.engine.impl.vector;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import prerna.engine.api.VectorDatabaseTypeEnum;
import prerna.om.Insight;

public class MilvusVectorDatabaseEngine extends AbstractVectorDatabaseEngine {

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		// add details for how to connect to the instance from the smss values
	}
	
	@Override
	public void addEmbeddings(VectorDatabaseCSVTable vectorCsvTable, Insight insight, Map<String, Object> parameters) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void removeDocument(List<String> fileNames, Map<String, Object> parameters) throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected List<Map<String, Object>> nearestNeighborCall(Insight insight, String searchStatement, Number limit, Map<String, Object> parameters) {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public List<Map<String, Object>> listDocuments(Map<String, Object> parameters) {
		// TODO: dont use the super which looks at the files in the OS
		// query the milvus instance for the documents it has
		return super.listDocuments(parameters);
		
//		String indexClass = this.defaultIndexClass;
//		if (parameters.containsKey("indexClass")) {
//			indexClass = (String) parameters.get("indexClass");
//		}
//
//		File documentsDir = new File(this.schemaFolder.getAbsolutePath() + DIR_SEPARATOR + indexClass + DIR_SEPARATOR + AbstractVectorDatabaseEngine.DOCUMENTS_FOLDER_NAME);
//		if(documentsDir.exists() && documentsDir.isDirectory()) {
//			for(Map<String, Object> fileInPostgresDb : sourcesInPostgresDb) {
//				String fileName = (String) fileInPostgresDb.get("fileName");
//				
//				File thisF = new File(documentsDir, fileName);
//				if(thisF.exists() && thisF.isFile()) {
//					long fileSizeInBytes = thisF.length();
//					double fileSizeInMB = (double) fileSizeInBytes / (1024);
//					SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//					String lastModified = dateFormat.format(new Date(thisF.lastModified()));
//
//					// add file size and last modified into the map
//					fileInPostgresDb.put("fileSize", fileSizeInMB);
//					fileInPostgresDb.put("lastModified", lastModified);
//				}
//			}
//		}
	}

	
	@Override
	public VectorDatabaseTypeEnum getVectorDatabaseType() {
		return null;
	}
}
