package prerna.io.connector.docs;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GoogleDocsListReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GoogleDocsListReactor.class);

    private static final String MIME_TYPE = "application/vnd.google-apps.document";
    private static final String DRIVE_API_URL = "https://www.googleapis.com/drive/v3/files";
    private static final String QUERY_PARAM_TEMPLATE = "mimeType='%s'";
    private static final String FIELDS_PARAM = "files(id,name)";
    private static final String AUTHORIZATION = "Authorization";
    private static final String DOCID_LIST = "docIdList";
    private static final String BEARER = "Bearer ";
    private static final String GET = "GET";
    private static final String FILES = "files";
    private static final String NAME = "name";
    private static final String ID = "id";
    
    private static final Gson gson = new Gson();

    @Override
    public NounMetadata execute() {
        try {
            User user = this.insight.getUser();
            String accessToken = GoogleDocsUtils.getGoogleAccessToken(user);
            List<List<String>> docidList = getDocsIdListUsingRest(accessToken);
            HashMap<String, Object> res = new HashMap<>();
			res.put(DOCID_LIST, docidList);
            return new NounMetadata(res, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
        } catch (Exception e) {
            classLogger.error("Unauthorized access or Please provide valid input", e);
            throw new SemossPixelException("Please provide valid input: " + e.getMessage(), e);
        }
    }
    
    @SuppressWarnings("unchecked")
    public static List<List<String>> getDocsIdListUsingRest(String accessToken) {
        List<List<String>> docList = new ArrayList<>();
        try {
            String queryParam = String.format(QUERY_PARAM_TEMPLATE, MIME_TYPE);
            String fullUrl = DRIVE_API_URL + "?q=" + java.net.URLEncoder.encode(queryParam, "UTF-8") + "&fields=" + java.net.URLEncoder.encode(FIELDS_PARAM, "UTF-8");
            HttpURLConnection conn = (HttpURLConnection) new URL(fullUrl).openConnection();
            conn.setRequestMethod(GET);
            conn.setRequestProperty(AUTHORIZATION, BEARER + accessToken);
            int responseCode = conn.getResponseCode();
            if (responseCode != 200) {
                classLogger.error("Failed to list Google Docs. Response Code: " + responseCode);
                throw new SemossPixelException("Drive API error: HTTP " + responseCode);
            }
            try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
                Map<String, Object> json = gson.fromJson(in, new TypeToken<Map<String, Object>>() {}.getType());
                List<Map<String, Object>> files = (List<Map<String, Object>>) json.get(FILES);
                for (Map<String, Object> file : files) {
                    String name = (String) file.get(NAME);
                    String id = (String) file.get(ID);
                    if (name != null && id != null) {
                        docList.add(Arrays.asList(name, id));
                    }
                }
            }
            conn.disconnect();
        } catch (Exception e) {
            classLogger.error("Failed to retrieve Google Docs file list from Drive REST API", e);
            throw new SemossPixelException("Failed to retrieve Google Docs file list from Drive REST API: " + e.getMessage(), e);
        }
        return docList;
    }

    @Override
    public String getReactorDescription() {
        return "This reactor is used to get the list of Google documents using Drive REST API.";
    }
}