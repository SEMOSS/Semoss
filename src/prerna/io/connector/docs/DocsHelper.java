package prerna.io.connector.docs;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.IOException;
import java.sql.*;

import com.google.api.services.docs.v1.Docs;
import com.google.api.services.docs.v1.model.BatchUpdateDocumentRequest;
import com.google.api.services.docs.v1.model.BatchUpdateDocumentResponse;
import com.google.api.services.docs.v1.model.Document;
import com.google.api.services.docs.v1.model.InsertTextRequest;
import com.google.api.services.docs.v1.model.ParagraphElement;
import com.google.api.services.docs.v1.model.StructuralElement;
import com.google.api.services.drive.Drive;

import prerna.auth.utils.AdminSecurityGroupUtils;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Constants;
import prerna.util.Utility;

import com.google.api.services.docs.v1.model.*;

public class DocsHelper {

	private static final Logger classLogger = LogManager.getLogger(AdminSecurityGroupUtils.class);
	static RDBMSNativeEngine securityDb = (RDBMSNativeEngine) Utility.getDatabase(Constants.SECURITY_DB);

	public static Document createDoc(Docs service, String title, String content, String name) throws Exception {
		
		if(titleExists(title) || title == null) {
			throw new IllegalArgumentException("Title " + title + " already exist");
		}
		Document doc = new Document().setTitle(title);
		doc = service.documents().create(doc).execute();
		Connection conn = null;
		try {
			conn = securityDb.makeConnection();
			String updatequery = "UPDATE GOOGLE_DOCS_PROFILE SET DOCID = ?, TITLE = ? WHERE NAME = ?";
			try (PreparedStatement ps = conn.prepareStatement(updatequery)) {
				ps.setString(1, doc.getDocumentId());
				ps.setString(2, title);
				ps.setString(3, name);
				int rowaffected = ps.executeUpdate();
				if (rowaffected > 0) {
					System.out.println("Document Id updated successfully.");
				} else {
					System.out.println("Problem in updating Document Id.");
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (securityDb.isConnectionPooling() && conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		if (content != null) {
			updateDoc(service, title, name, content, 1);
		}
		return doc;
	}

	public static String readDoc(Docs service, String title, String name) throws Exception {
		String id = getDocIdByTitle(title, name);
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

	public static Boolean updateDoc(Docs service, String title, String name, String content, int sindex) throws Exception {
		String id = getDocIdByTitle(title, name);
		try {
			List<Request> requests = new ArrayList<>();
			BatchUpdateDocumentRequest body = null;
			BatchUpdateDocumentResponse response = null;
			requests.add(new Request().setInsertText(
					new InsertTextRequest().setText(content).setLocation(new Location().setIndex(sindex))));
			body = new BatchUpdateDocumentRequest().setRequests(requests);
			response = service.documents().batchUpdate(id, body).execute();
			return true;

		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	public static Boolean deleteDoc(Docs service, Drive driveService, String title, String name, Integer sindex, Integer eindex)
			throws Exception {
		String id = getDocIdByTitle(title, name);
		try {
			if (sindex == null && eindex == null) {
				DocsDelete(driveService, id);
				return true;
			} else {
				List<Request> requests = new ArrayList<>();
				BatchUpdateDocumentRequest body = null;
				BatchUpdateDocumentResponse response = null;
				requests.add(new Request().setDeleteContentRange(new DeleteContentRangeRequest()
						.setRange(new Range().setStartIndex(sindex).setEndIndex(eindex))));
				body = new BatchUpdateDocumentRequest().setRequests(requests);
				response = service.documents().batchUpdate(id, body).execute();
				return true;
			}

		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	public static String getDocIdByTitle(String title, String name) {

		Connection conn = null;
		String docId = null;

		try {
			conn = securityDb.makeConnection();
			String query = "SELECT DOCID FROM GOOGLE_DOCS_PROFILE WHERE TITLE = ? AND NAME = ?";

			try (PreparedStatement ps = conn.prepareStatement(query)) {
				ps.setString(1, title);
				ps.setString(2, name);
				try (ResultSet rs = ps.executeQuery()) {
					if (rs.next()) {
						docId = rs.getString("DOCID");
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (securityDb.isConnectionPooling() && conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		return docId;
	}

	public static void DocsDelete(Drive driveService, String id) throws Exception {
		driveService.files().delete(id).execute();
		Connection conn = null;
		try {
			conn = securityDb.makeConnection();
			String deletequery = "DELETE FROM GOOGLE_DOCS_PROFILE WHERE DOCID = ? ";
			try (PreparedStatement ps = conn.prepareStatement(deletequery)) {
				ps.setString(1, id);
				int rowaffected = ps.executeUpdate();
				if (rowaffected > 0) {
					String message = "Row with docid " + id + " succesfully deleted";
					System.out.println(message);
				} else {
					String message = "Error in deleting the row";
					System.out.println(message);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			String error = "Error in getting the user ids";
			System.out.println(error);
		} finally {
			if (securityDb.isConnectionPooling() && conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}
	
	public static boolean titleExists(String title) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GOOGLE_DOCS_PROFILE__TITLE"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GOOGLE_DOCS_PROFILE__TITLE", "==", title));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if(wrapper.hasNext()) {
				return true;
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}

		return false;
	}
}
