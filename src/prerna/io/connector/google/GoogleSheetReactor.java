package prerna.io.connector.google;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.sheets.v4.Sheets;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.SheetServiceUtil;
import prerna.util.SpreadSheetHelper;

public class GoogleSheetReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GoogleSheetReactor.class);

	public GoogleSheetReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COMMAND.getKey(), ReactorKeysEnum.TITLESHEET_NAME.getKey(), ReactorKeysEnum.SHEET_NAME.getKey(), ReactorKeysEnum.ROW_NO.getKey(),ReactorKeysEnum.COLUMN_NO.getKey(), ReactorKeysEnum.DATA.getKey() };
		this.keyRequired = new int[] { 1, 1, 0, 0, 0, 0};
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String command = this.keyValue.get(this.keysToGet[0]);
		String titleSheetName = this.keyValue.get(this.keysToGet[1]);
		String sheetName = this.keyValue.get(this.keysToGet[2]);
		String rowNo = this.keyValue.get(this.keysToGet[3]);
		String colNo = this.keyValue.get(this.keysToGet[4]);
		String data = this.keyValue.get(this.keysToGet[5]);
//		String accessToken=getAccessToken();
		String accessToken="ya29.a0AS3H6NxGF5uE4EDiIIruEyhY3YWAf2dyrtVwim5_BSYvmaMOoas6ICjNxfIL5OHJb4wsyfQu4szJ8Tly5DOLuFQQ4ko1FVnZEIe-MkC3e4GQrqJTRFt_MW-9p2b1DEjszyAwrr8MeQBhUfTmoPUrIBkX6e2-HXOzgJye_LnwaCgYKAacSARcSFQHGX2MiTpMOmqEPtYMtFjIc50ccjw0175";
		System.out.println("accessToken: "+accessToken);
		if (this.keyValue.get(this.keysToGet[5]) != null && this.keyValue.get(this.keysToGet[5]) != "") {
			data = this.keyValue.get(this.keysToGet[5]);
		}
		Sheets sheetsService = null;
		try {
			switch (command.trim().toLowerCase()) {
			case "write": 
				return SpreadSheetHelper.writeData(titleSheetName, sheetName, rowNo, colNo, data, accessToken);
			case "update":
				return SpreadSheetHelper.updateData(titleSheetName, sheetName, rowNo, colNo, data, accessToken); 
			case "delete":
				return SpreadSheetHelper.deleteData(titleSheetName, sheetName, rowNo, colNo, accessToken);
			case "read":
				return SpreadSheetHelper.readData(titleSheetName, sheetName, rowNo, colNo, data, accessToken);
			case "delete sheet":
				return SpreadSheetHelper.deleteSheet(titleSheetName, sheetName, accessToken);
			case "create new spread sheet":
				return SpreadSheetHelper.createnewSpreadSheet(titleSheetName, accessToken); 
			case "create new sheet":
				return SpreadSheetHelper.createnewSheet(titleSheetName,sheetName, accessToken); 
			case "truncate DB data":
				return SpreadSheetHelper.truncateData(accessToken); 
			case "Delete DB record for user id":
				return SpreadSheetHelper.deleteRecordUserId(accessToken); 
			case "delete titlesheet":
				return SpreadSheetHelper.deleteTitleSheet(titleSheetName, accessToken); 
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("Issue with input");
		}
		return new NounMetadata("Please provide valid command", PixelDataType.CUSTOM_DATA_STRUCTURE,
				PixelOperationType.OPERATION);
	}

	private String getAccessToken() {
		String accessToken=null;
		User user = this.insight.getUser();
		try {
			if(user==null) {
				Map<String,Object> retMap=new HashMap<String, Object>();
				retMap.put("type", "google");
				retMap.put("message", "Please login to your Google account");
				throwLoginError(retMap);	
			}else {
				AccessToken msToken = user.getAccessToken(AuthProvider.GOOGLE);
				accessToken=msToken.getAccess_token();
			}
		}catch(Exception e) {
			Map<String, Object> retMap = new HashMap<>();
			retMap.put("type", "google");
			retMap.put("message", "Please login to your Google account");
			throwLoginError(retMap);
		}
		return accessToken;
	}

}
