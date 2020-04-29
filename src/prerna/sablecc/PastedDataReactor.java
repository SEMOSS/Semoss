package prerna.sablecc;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import prerna.poi.main.helper.CSVFileHelper;
import prerna.sablecc.meta.FilePkqlMetadata;
import prerna.sablecc.meta.IPkqlMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class PastedDataReactor extends AbstractReactor {

	public static final String DELIMITER = "delimiter";
	protected static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	
	private String fileName;
	
	public PastedDataReactor() {
		String [] thisReacts = {PKQLEnum.ROW_CSV, PKQLEnum.FILTER, PKQLEnum.JOINS, DELIMITER};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.PASTED_DATA;
	}

	@Override
	public Iterator process() {
		System.out.println("Processed.. " + myStore);
		
		// save the file with the date
		Date date = new Date();
		String modifiedDate = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss_SSSS").format(date);
		String fileInfo = myStore.get(PKQLEnum.PASTED_DATA).toString().replace("<startInput>", "").replace("<endInput>", "");
	
		fileName = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR + "PastedData" + modifiedDate;
		File file = new File(fileName);
		FileWriter fw = null;
		try {
			fw = new FileWriter(file);
			fw.write(fileInfo);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if(fw != null) {
				try {
					fw.flush();
					fw.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		System.out.println( "Saved Filename: " + fileName);
		
		// create a helper to get the headers for qs and edge hash
		CSVFileHelper helper = new CSVFileHelper();
		String delimiter = myStore.get(DELIMITER).toString();
		helper.setDelimiter(delimiter.charAt(0));
		helper.parse(fileName);

		String[] headers = helper.getHeaders();
		this.put(PKQLEnum.COL_CSV, Arrays.asList(headers));
		
//		Map<String, Set<String>> edgeHash = TinkerMetaHelper.createPrimKeyEdgeHash(headers);
//		this.put("EDGE_HASH", edgeHash);
//		
//		QueryStruct qs = new QueryStruct();
//		for(String header : headers) {
//			qs.addSelector(header, null);
//		}
//		Iterator it = CsvFileIterator.createInstance(IFileIterator.FILE_DATA_TYPE.STRING, fileName, delimiter.charAt(0), qs, null, null);
//		
//		String nodeStr = (String) myStore.get(whoAmI);
//		myStore.put(nodeStr, it);
	
		return null;
	}
	
	public IPkqlMetadata getPkqlMetadata() {
		FilePkqlMetadata fileData = new FilePkqlMetadata();
		fileData.setFileLoc(this.fileName);
		fileData.setDataMap(null);
		fileData.setSelectors((List<String>) getValue(PKQLEnum.COL_CSV));
		fileData.setTableJoin((List<Map<String, Object>>) getValue(PKQLEnum.TABLE_JOINS));
		fileData.setPkqlStr((String) getValue(PKQLEnum.PASTED_DATA));
		fileData.setType(FilePkqlMetadata.FILE_TYPE.CSV);

		return fileData;
	}
}
