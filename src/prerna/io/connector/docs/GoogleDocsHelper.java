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

		if (title == null || titleExists(driveService, title)) {
		    throw new IllegalArgumentException("Title " + title + " already exists");
		}
		Document doc = new Document().setTitle(title);
		doc = service.documents().create(doc).execute();
		if (content != null) {
			String id = doc.getDocumentId();
			updateDoc(service, id, content);
		}
		return doc;
	}

	public static String readDoc(Docs service, String id) throws Exception {
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

	public static Boolean updateDoc(Docs service, String id, String content) throws Exception {
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

	public static Boolean deleteDoc(Drive driveService, String id) throws Exception {
		try {
			driveService.files().delete(id).execute();
			System.out.println("Document with id " + id + " deleted successfully");
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

	}

	public static boolean titleExists(Drive driveService, String title) {
		final String MIME_TYPE = "application/vnd.google-apps.document";
		final String FIELDS = "files(id)";
		try {
			String query = String.format("name = '%s' and mimeType = '%s'", title, MIME_TYPE);

			FileList result = driveService.files().list().setQ(query).setFields(FIELDS).execute();

			List<File> files = result.getFiles();
			return files != null && !files.isEmpty();

		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}
}
