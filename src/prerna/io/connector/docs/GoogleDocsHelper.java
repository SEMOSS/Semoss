package prerna.io.connector.docs;

import java.util.ArrayList;
import java.util.List;
import java.io.IOException;

import com.google.api.services.docs.v1.Docs;
import com.google.api.services.docs.v1.model.BatchUpdateDocumentRequest;
import com.google.api.services.docs.v1.model.Document;
import com.google.api.services.docs.v1.model.InsertTextRequest;
import com.google.api.services.docs.v1.model.ParagraphElement;
import com.google.api.services.docs.v1.model.StructuralElement;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.model.*;
import com.google.api.services.docs.v1.model.*;

public class GoogleDocsHelper {

	public static Document createDoc(Docs service, Drive driveService, String title, String content) throws Exception {

		if (titleExists(driveService, title) || title == null) {
			throw new IllegalArgumentException("Title " + title + " already exist");
		}
		Document doc = new Document().setTitle(title);
		doc = service.documents().create(doc).execute();
		if (content != null) {
			updateDoc(service, driveService, title, content);
		}
		return doc;
	}

	public static String readDoc(Docs service, Drive driveService, String title) throws Exception {
		String id = getDocIdByTitle(driveService, title);
		Document doc = service.documents().get(id).execute();
		StringBuilder sb = new StringBuilder();

		for (StructuralElement e : doc.getBody().getContent()) {
			if (e.getParagraph() != null) {
				for (ParagraphElement pe : e.getParagraph().getElements()) {
					if (pe.getTextRun() != null) {
						sb.append(pe.getTextRun().getContent());
					}
				}
			}
		}
		return sb.toString();
	}

	public static Boolean updateDoc(Docs service, Drive driveService, String title, String content) throws Exception {
		String id = getDocIdByTitle(driveService, title);
		try {
			List<Request> requests = new ArrayList<>();
			Document doc = service.documents().get(id).execute();
			List<StructuralElement> contents = doc.getBody().getContent();
			int endIndex = contents.get(contents.size() - 1).getEndIndex();
			int startIndex = 1;
			int deleteEndIndex = endIndex - 1;
			if (deleteEndIndex > startIndex) {
				requests.add(new Request().setDeleteContentRange(new DeleteContentRangeRequest()
						.setRange(new Range().setStartIndex(startIndex).setEndIndex(deleteEndIndex))));
			}

			requests.add(new Request()
					.setInsertText(new InsertTextRequest().setText(content).setLocation(new Location().setIndex(1))));

			BatchUpdateDocumentRequest body = new BatchUpdateDocumentRequest().setRequests(requests);
			service.documents().batchUpdate(id, body).execute();
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	public static Boolean deleteDoc(Drive driveService, String title) throws Exception {
		String id = getDocIdByTitle(driveService, title);
		try {
			driveService.files().delete(id).execute();
			System.out.println("Document with id " + id + " deleted successfully");
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

	}
	
	public static String getDocIdByTitle(Drive driveService, String title) {
		String docId = null;
		try {
			String query = String.format("name = '%s' and mimeType = 'application/vnd.google-apps.document'", title);

			FileList result = driveService.files().list().setQ(query).setFields("files(id, name)").execute();

			List<File> files = result.getFiles();
			if (files != null && !files.isEmpty()) {
				docId = files.get(0).getId();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return docId;
	}

	public static boolean titleExists(Drive driveService, String title) {
		try {
			String query = String.format("name = '%s' and mimeType = 'application/vnd.google-apps.document'", title);

			FileList result = driveService.files().list().setQ(query).setFields("files(id)").execute();

			List<File> files = result.getFiles();
			return files != null && !files.isEmpty();

		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}
}
