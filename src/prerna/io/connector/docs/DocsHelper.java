package prerna.io.connector.docs;

import java.util.ArrayList;
import java.util.List;

import com.google.api.services.docs.v1.Docs;
import com.google.api.services.docs.v1.model.BatchUpdateDocumentRequest;
import com.google.api.services.docs.v1.model.BatchUpdateDocumentResponse;
import com.google.api.services.docs.v1.model.Document;
import com.google.api.services.docs.v1.model.InsertTextRequest;
import com.google.api.services.docs.v1.model.ParagraphElement;
import com.google.api.services.docs.v1.model.StructuralElement;

import prerna.engine.api.IDatabaseEngine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

import com.google.api.services.docs.v1.model.*;

public class DocsHelper {
	
	public static Document createDoc(Docs service, String title) throws Exception{
		Document doc = new Document().setTitle(title);
	    doc = service.documents().create(doc).execute();
	    return doc;
	}
	
	public static String readDoc(Docs service, String id) throws Exception{
		Document doc = service.documents().get(id).execute();
		StringBuilder sb = new StringBuilder();
		
		for(StructuralElement e : doc.getBody().getContent()) {
			if(e.getParagraph() != null) {
				for(ParagraphElement pe : e.getParagraph().getElements()) {
					if(pe.getTextRun() != null) {
						sb.append(pe.getTextRun().getContent());
					}
				}
			}
		}
		return sb.toString();
	}
	
	public static Boolean updateDoc(Docs service, String id, String content, int sindex) throws Exception{
		try {
			List<Request> requests = new ArrayList<>();
			BatchUpdateDocumentRequest body = null;
			BatchUpdateDocumentResponse response = null;
			requests.add(new Request().setInsertText(new InsertTextRequest().setText(content).setLocation(new Location().setIndex(sindex))));
			body = new BatchUpdateDocumentRequest().setRequests(requests);
			response = service.documents().batchUpdate(id, body).execute();
			return true;
			
		}
		catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}
	
	public static Boolean deleteDoc(Docs service, String id, int sindex, int eindex) throws Exception{
		try {
			List<Request> requests = new ArrayList<>();
			BatchUpdateDocumentRequest body = null;
			BatchUpdateDocumentResponse response = null;
			requests.add(new Request().setDeleteContentRange(new DeleteContentRangeRequest().setRange(new Range().setStartIndex(sindex).setEndIndex(eindex))));
			body = new BatchUpdateDocumentRequest().setRequests(requests);
			response = service.documents().batchUpdate(id, body).execute();
			return true;
			
		}
		catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}


}
