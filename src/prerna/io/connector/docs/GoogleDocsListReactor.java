package prerna.io.connector.docs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;

import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GoogleDocsListReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		try {
			User user = this.insight.getUser();
			String accessToken = GoogleDocsUtils.getGoogleAccessToken(user);
			Drive getDriveService = GoogleDocsUtils.getDriveServiceUsingToken(accessToken);

			List<List<String>> docIdList = getDocsIdList(getDriveService);
			HashMap<String, Object> res = new HashMap<>();
			res.put("DocIdList", docIdList);
			return new NounMetadata(res, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			throw new SemossPixelException("Please provide valid input", e);
		}

	}

	public static List<List<String>> getDocsIdList(Drive driveService) {
		List<List<String>> docList = new ArrayList<>();
		final String MIME_TYPE = "application/vnd.google-apps.document";
		try {
			String query = String.format("mimeType = '%s'", MIME_TYPE);

			FileList result = driveService.files().list().setQ(query).setFields("files(id, name)").execute();

			List<File> files = result.getFiles();

			if (files != null && !files.isEmpty()) {
				for (File file : files) {
					List<String> lst = new ArrayList<>();
					lst.add(file.getName());
					lst.add(file.getId());
					docList.add(lst);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return docList;
	}

	@Override
	public String getReactorDescription() {
		return "This reactor is used to get the list of Google document.";
	}

}
