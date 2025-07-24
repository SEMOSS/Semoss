package prerna.io.connector.docs;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
	
	private static final Logger classLogger = LogManager.getLogger(GoogleDocsHelper.class);

	public static Document createDoc(Docs service, Drive driveService, String title, String content) throws Exception {

		if (title == null || titleExists(driveService, title)) {
			classLogger.error("Title already exists");
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

	public static Boolean updateDoc(Docs service, String id, String content) {
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
			classLogger.error("Failed to update Google Doc");
			return false;
		}
	}

	public static Boolean deleteDoc(Drive driveService, String id) {
		try {
			driveService.files().delete(id).execute();
			return true;
		} catch (Exception e) {
			classLogger.error("Failed to delete Google Doc");
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
			classLogger.error("Error checking existence of Google Doc with title");
			return false;
		}
	}
}
