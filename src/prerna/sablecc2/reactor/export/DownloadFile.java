package prerna.sablecc2.reactor.export;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.poi.ss.usermodel.Workbook;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class DownloadFile extends AbstractReactor {

	public DownloadFile() {
		// keep open specifies whether to keep this open or close it. if kept open then
		// this will return open as noun metadata
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_NAME.getKey() };
	}

	@Override
	public NounMetadata execute() {
		// TODO Auto-generated method stub
		String fileName = null;
		organizeKeys();

		try {
			if (keyValue.containsKey(keysToGet[0]))
				fileName = keyValue.get(keysToGet[0]);

			Map<String, Object> exportMap = (Map) insight.getVar(insight.getInsightId());
			// get the workbook
			if(fileName == null)
				fileName = (String)exportMap.get("FILE_NAME");
			Workbook wb = (Workbook) exportMap.get(fileName);

			String exportName = AbstractExportTxtReactor.getExportFileName(fileName, "xlsx");
			File file = new File(insight.getInsightFolder());
			if (!file.exists())
				file.mkdirs();
			String fileLocation = insight.getInsightFolder() + DIR_SEPARATOR + exportName;

			FileOutputStream fileOut = new FileOutputStream(fileLocation);
			wb.write(fileOut);
			insight.getVarStore().remove(insight.getInsightId());
			
			this.insight.addExportFile(exportName, fileLocation);
			NounMetadata retNoun = new NounMetadata(exportName, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD);
			
			return retNoun;
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	

}
