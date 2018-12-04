package prerna.sablecc2.reactor.app.upload;

import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import prerna.poi.main.HeadersException;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class CheckHeadersReactor extends AbstractReactor {

	public CheckHeadersReactor() {
		this.keysToGet = new String[] { "headerMap" };
	}

	@Override
	public NounMetadata execute() {
		Map<String, Object> headerMap = (Map) this.curRow.get(0);
		if (headerMap == null || headerMap.isEmpty()) {
			throw new IllegalArgumentException("Need to define " + this.keysToGet[0]);
		}

		HeadersException headerChecker = HeadersException.getInstance();
		Map<String, Map<String, String>> invalidHeadersMap = new Hashtable<String, Map<String, String>>();
		for (String sheetName : headerMap.keySet()) {
			List<String> userHeadersList = (List<String>) headerMap.get(sheetName);
			String[] userHeaders = userHeadersList.toArray(new String[userHeadersList.size()]);

			// now we need to check all of these headers
			for (int colIdx = 0; colIdx < userHeaders.length; colIdx++) {
				String userHeader = userHeaders[colIdx];
				Map<String, String> badHeaderMap = new Hashtable<String, String>();
				if (headerChecker.isIllegalHeader(userHeader)) {
					badHeaderMap.put(userHeader, "This header name is a reserved word");
				} else if (headerChecker.containsIllegalCharacter(userHeader)) {
					badHeaderMap.put(userHeader, "Header names cannot contain +%@;");
				} else if (headerChecker.isDuplicated(userHeader, userHeaders, colIdx)) {
					badHeaderMap.put(userHeader, "Cannot have duplicate header names");
				}

				// map is filled in only if the header is bad
				if (!badHeaderMap.isEmpty()) {
					Map<String, String> invalidHeadersForFile = null;
					if (invalidHeadersMap.containsKey(sheetName)) {
						invalidHeadersForFile = invalidHeadersMap.get(sheetName);
					} else {
						invalidHeadersForFile = new Hashtable<String, String>();
					}

					// now add in the bad header for the sheet map
					invalidHeadersForFile.putAll(badHeaderMap);
					// now store it in the overall object
					invalidHeadersMap.put(sheetName, invalidHeadersForFile);
				}
			}

		}
		if (invalidHeadersMap.isEmpty()) {
			return new NounMetadata(true, PixelDataType.BOOLEAN);
		} else {
			NounMetadata noun = new NounMetadata("Invalid Headers", PixelDataType.CONST_STRING, PixelOperationType.ERROR);
			noun.addAdditionalReturn(new NounMetadata(invalidHeadersMap, PixelDataType.MAP));
			SemossPixelException exception = new SemossPixelException(noun);
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}

	}
}
