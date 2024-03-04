package api.prerna.engine;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Test;

import api.ApiSemossTestEmailUtils;
import api.ApiSemossTestEngineUtils;
import api.ApiSemossTestUserUtils;
import api.ApiSemossTestUtils;
import api.BaseSemossApiTests;
import prerna.auth.AccessPermissionEnum;
import prerna.auth.utils.SecurityEngineUtils;
//import prerna.reactor.engine.EnginePermissionHistoryReactor;
import prerna.reactor.engine.GetEngineUserAccessRequestReactor;
import prerna.reactor.engine.RequestEngineReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class RequestEngineAccessTests extends BaseSemossApiTests {
	
	@SuppressWarnings("unchecked")
	@Test
	public void testRequest() {
		// make engine get engine id
		Map<String, String> additionalDataTypes = new HashMap<>();
		additionalDataTypes.put("datesubmitted", "yyyy-MM-d H:mm:ss");

		String engineId = ApiSemossTestEngineUtils.addTestRdbmsDatabase(
				"testEngineRequestDB",
				Arrays.asList("yes", "no", "maybeso", "datesubmitted"),
				Arrays.asList("INT", "INT", "INT", "TIMESTAMP"),
				additionalDataTypes,
				Arrays.asList(Arrays.asList("1", "0", "0", "2022-09-13 12:00:01"))
				);
		
		// No reactor, so just use Utility class in src code.
		// Other option is to make a Http Request, which is kind of hard
		try {
			SecurityEngineUtils.setEngineDiscoverable(ApiSemossTestUserUtils.getUser(), engineId, true);
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail("could not set engine discoverable");
		}

		// make another user
		ApiSemossTestUserUtils.addAndSetNewNativeUser("user2", false);
		
		// request access from second user to engine
		String pc2 = ApiSemossTestUtils.buildPixelCall(RequestEngineReactor.class, "engine", engineId, "permission", "2", "comment", "this is a test request");
		ApiSemossTestUtils.processPixel(pc2);
		
		// verify email sent
		List<Map<String, Object>> emails = ApiSemossTestEmailUtils.getAllEmails();
		assertEquals(1, emails.size());
		Map<String, Object> email = emails.get(0);
		assertEquals("ater@ater.com", ((List<Map<String, Object>>) email.get("To")).get(0).get("Address"));
		assertEquals("semossemailtest@gmail.com", ((Map<String, Object>) email.get("From")).get("Address"));
		
		Map<String, Object> emailDetails = ApiSemossTestEmailUtils.getEmail(email.get("ID").toString());
		assertEquals("<!doctype html>\r\n"
				+ "<html lang=\"en-US\">\r\n"
				+ "\r\n"
				+ "<head>\r\n"
				+ "    <meta content=\"text/html; charset=utf-8\" http-equiv=\"Content-Type\" />\r\n"
				+ "    <title>Engine Access Request</title>\r\n"
				+ "    <meta name=\"description\" content=\"Engine Access Request\">\r\n"
				+ "    <style type=\"text/css\">\r\n"
				+ "        a:hover {text-decoration: underline !important;}\r\n"
				+ "    </style>\r\n"
				+ "</head>\r\n"
				+ "\r\n"
				+ "<body marginheight=\"0\" topmargin=\"0\" marginwidth=\"0\" style=\"margin: 0px; background-color: #f2f3f8;\" leftmargin=\"0\">\r\n"
				+ "    <!--100% body table-->\r\n"
				+ "    <table cellspacing=\"0\" border=\"0\" cellpadding=\"0\" width=\"100%\" bgcolor=\"#f2f3f8\"\r\n"
				+ "        style=\"@import url(https://fonts.googleapis.com/css?family=Rubik:300,400,500,700|Open+Sans:300,400,600,700); font-family: 'Open Sans', sans-serif;\">\r\n"
				+ "        <tr>\r\n"
				+ "            <td>\r\n"
				+ "                <table style=\"background-color: #f2f3f8; max-width:670px;  margin:0 auto;\" width=\"100%\" border=\"0\"\r\n"
				+ "                    align=\"center\" cellpadding=\"0\" cellspacing=\"0\">\r\n"
				+ "                    <tr>\r\n"
				+ "                        <td style=\"height:80px;\">&nbsp;</td>\r\n"
				+ "                    </tr>\r\n"
				+ "                    <tr>\r\n"
				+ "                        <td style=\"height:20px;\">&nbsp;</td>\r\n"
				+ "                    </tr>\r\n"
				+ "                    <tr>\r\n"
				+ "                        <td>\r\n"
				+ "                            <table width=\"95%\" border=\"0\" align=\"center\" cellpadding=\"0\" cellspacing=\"0\"\r\n"
				+ "                                style=\"max-width:670px;background:#fff; border-radius:3px; text-align:center;-webkit-box-shadow:0 6px 18px 0 rgba(0,0,0,.06);-moz-box-shadow:0 6px 18px 0 rgba(0,0,0,.06);box-shadow:0 6px 18px 0 rgba(0,0,0,.06);\">\r\n"
				+ "                                <tr>\r\n"
				+ "                                    <td style=\"height:40px;\">&nbsp;</td>\r\n"
				+ "                                </tr>\r\n"
				+ "                                <tr>\r\n"
				+ "                                    <td style=\"padding:0 35px;\">\r\n"
				+ "                                        <h1 style=\"color:#1e1e2d; font-weight:500; margin:0;font-size:32px;font-family:'Rubik',sans-serif;\">Engine Access Request</h1>\r\n"
				+ "                                        <span\r\n"
				+ "                                            style=\"display:inline-block; vertical-align:middle; margin:29px 0 26px; border-bottom:1px solid #cecece; width:100px;\"></span>\r\n"
				+ "                                        <p style=\"color:#455056; font-size:15px;line-height:24px; margin:0;\">\r\n"
				+ "                                            The following user has requested access for your Engine: <strong>testEngineRequestDB</strong><br><br>\r\n"
				+ "                                            <strong>Username:</strong> <br>\r\n"
				+ "                                            <strong>Email:</strong> user2@user2.com<br>\r\n"
				+ "                                            <strong>Permission:</strong> EDIT<br>\r\n"
				+ "                                            <strong>Reason: </strong>this is a test request\r\n"
				+ "                                        </p>\r\n"
				+ "                                    </td>\r\n"
				+ "                                </tr>\r\n"
				+ "                                <tr>\r\n"
				+ "                                    <td style=\"height:40px;\">&nbsp;</td>\r\n"
				+ "                                </tr>\r\n"
				+ "                            </table>\r\n"
				+ "                        </td>\r\n"
				+ "                    </tr>\r\n"
				+ "                    <tr>\r\n"
				+ "                        <td style=\"height:20px;\">&nbsp;</td>\r\n"
				+ "                    </tr>\r\n"
				+ "                </table>\r\n"
				+ "            </td>\r\n"
				+ "        </tr>\r\n"
				+ "    </table>\r\n"
				+ "    <!--/100% body table-->\r\n"
				+ "</body>\r\n"
				+ "\r\n"
				+ "</html>\r\n"
				+ "", emailDetails.get("HTML").toString());
		assertEquals("SEMOSS - Database Access Request", emailDetails.get("Subject").toString());

		// Go back to the default user
		ApiSemossTestUserUtils.setDefaultTestUser();
		
		// Get Requests
		String pc3 = ApiSemossTestUtils.buildPixelCall(GetEngineUserAccessRequestReactor.class, "engine", engineId);
		NounMetadata engineRequestNM = ApiSemossTestUtils.processPixel(pc3);
		List<Map<String, Object>> requestOutput = (List<Map<String, Object>>) engineRequestNM.getValue();
		assertEquals(1, requestOutput.size());
		Map<String, Object> requestDetails = requestOutput.get(0);
		assertEquals(2, requestDetails.get("PERMISSION"));
		assertNotNull(requestDetails.get("REQUEST_TIMESTAMP"));
		assertEquals("NEW_REQUEST", requestDetails.get("APPROVER_DECISION").toString());
		assertNull(requestDetails.get("APPROVER_TIMESTAMP"));
		assertEquals("user2@user2.com", requestDetails.get("EMAIL").toString());
		assertEquals("u", requestDetails.get("NAME").toString());
		assertNull(requestDetails.get("APPROVER_TYPE"));
		assertEquals(engineId, requestDetails.get("ENGINEID").toString());
		assertEquals("user2", requestDetails.get("USERNAME").toString());
		assertEquals("user2", requestDetails.get("REQUEST_USERID").toString());
		assertEquals("Native", requestDetails.get("REQUEST_TYPE").toString());
		assertNotNull(requestDetails.get("ID"));
		assertNull(requestDetails.get("APPROVER_USERID"));
	}
	
	

	@SuppressWarnings("unchecked")
	@Test
	public void testApprove() {
		// make engine get engine id
		Map<String, String> additionalDataTypes = new HashMap<>();
		additionalDataTypes.put("datesubmitted", "yyyy-MM-d H:mm:ss");

		String engineId = ApiSemossTestEngineUtils.addTestRdbmsDatabase(
				"testApproveEngineRequestDB",
				Arrays.asList("yes", "no", "maybeso", "datesubmitted"),
				Arrays.asList("INT", "INT", "INT", "TIMESTAMP"),
				additionalDataTypes,
				Arrays.asList(Arrays.asList("1", "0", "0", "2022-09-13 12:00:01"))
				);
		
		// No reactor, so just use Utility class in src code.
		// Other option is to make a Http Request, which is kind of hard
		try {
			SecurityEngineUtils.setEngineDiscoverable(ApiSemossTestUserUtils.getUser(), engineId, true);
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail("could not set engine discoverable");
		}

		// make another user
		ApiSemossTestUserUtils.addAndSetNewNativeUser("user2", false);
		
		// request access from second user to engine
		String pc2 = ApiSemossTestUtils.buildPixelCall(RequestEngineReactor.class, "engine", engineId, "permission", "2", "comment", "this is a test request");
		ApiSemossTestUtils.processPixel(pc2);
		
		// verify email sent
		List<Map<String, Object>> emails = ApiSemossTestEmailUtils.getAllEmails();
		assertEquals(1, emails.size());
		Map<String, Object> email = emails.get(0);
		assertEquals("ater@ater.com", ((List<Map<String, Object>>) email.get("To")).get(0).get("Address"));
		assertEquals("semossemailtest@gmail.com", ((Map<String, Object>) email.get("From")).get("Address"));
		
		Map<String, Object> emailDetails = ApiSemossTestEmailUtils.getEmail(email.get("ID").toString());
		assertEquals("<!doctype html>\r\n"
				+ "<html lang=\"en-US\">\r\n"
				+ "\r\n"
				+ "<head>\r\n"
				+ "    <meta content=\"text/html; charset=utf-8\" http-equiv=\"Content-Type\" />\r\n"
				+ "    <title>Engine Access Request</title>\r\n"
				+ "    <meta name=\"description\" content=\"Engine Access Request\">\r\n"
				+ "    <style type=\"text/css\">\r\n"
				+ "        a:hover {text-decoration: underline !important;}\r\n"
				+ "    </style>\r\n"
				+ "</head>\r\n"
				+ "\r\n"
				+ "<body marginheight=\"0\" topmargin=\"0\" marginwidth=\"0\" style=\"margin: 0px; background-color: #f2f3f8;\" leftmargin=\"0\">\r\n"
				+ "    <!--100% body table-->\r\n"
				+ "    <table cellspacing=\"0\" border=\"0\" cellpadding=\"0\" width=\"100%\" bgcolor=\"#f2f3f8\"\r\n"
				+ "        style=\"@import url(https://fonts.googleapis.com/css?family=Rubik:300,400,500,700|Open+Sans:300,400,600,700); font-family: 'Open Sans', sans-serif;\">\r\n"
				+ "        <tr>\r\n"
				+ "            <td>\r\n"
				+ "                <table style=\"background-color: #f2f3f8; max-width:670px;  margin:0 auto;\" width=\"100%\" border=\"0\"\r\n"
				+ "                    align=\"center\" cellpadding=\"0\" cellspacing=\"0\">\r\n"
				+ "                    <tr>\r\n"
				+ "                        <td style=\"height:80px;\">&nbsp;</td>\r\n"
				+ "                    </tr>\r\n"
				+ "                    <tr>\r\n"
				+ "                        <td style=\"height:20px;\">&nbsp;</td>\r\n"
				+ "                    </tr>\r\n"
				+ "                    <tr>\r\n"
				+ "                        <td>\r\n"
				+ "                            <table width=\"95%\" border=\"0\" align=\"center\" cellpadding=\"0\" cellspacing=\"0\"\r\n"
				+ "                                style=\"max-width:670px;background:#fff; border-radius:3px; text-align:center;-webkit-box-shadow:0 6px 18px 0 rgba(0,0,0,.06);-moz-box-shadow:0 6px 18px 0 rgba(0,0,0,.06);box-shadow:0 6px 18px 0 rgba(0,0,0,.06);\">\r\n"
				+ "                                <tr>\r\n"
				+ "                                    <td style=\"height:40px;\">&nbsp;</td>\r\n"
				+ "                                </tr>\r\n"
				+ "                                <tr>\r\n"
				+ "                                    <td style=\"padding:0 35px;\">\r\n"
				+ "                                        <h1 style=\"color:#1e1e2d; font-weight:500; margin:0;font-size:32px;font-family:'Rubik',sans-serif;\">Engine Access Request</h1>\r\n"
				+ "                                        <span\r\n"
				+ "                                            style=\"display:inline-block; vertical-align:middle; margin:29px 0 26px; border-bottom:1px solid #cecece; width:100px;\"></span>\r\n"
				+ "                                        <p style=\"color:#455056; font-size:15px;line-height:24px; margin:0;\">\r\n"
				+ "                                            The following user has requested access for your Engine: <strong>testApproveEngineRequestDB</strong><br><br>\r\n"
				+ "                                            <strong>Username:</strong> <br>\r\n"
				+ "                                            <strong>Email:</strong> user2@user2.com<br>\r\n"
				+ "                                            <strong>Permission:</strong> EDIT<br>\r\n"
				+ "                                            <strong>Reason: </strong>this is a test request\r\n"
				+ "                                        </p>\r\n"
				+ "                                    </td>\r\n"
				+ "                                </tr>\r\n"
				+ "                                <tr>\r\n"
				+ "                                    <td style=\"height:40px;\">&nbsp;</td>\r\n"
				+ "                                </tr>\r\n"
				+ "                            </table>\r\n"
				+ "                        </td>\r\n"
				+ "                    </tr>\r\n"
				+ "                    <tr>\r\n"
				+ "                        <td style=\"height:20px;\">&nbsp;</td>\r\n"
				+ "                    </tr>\r\n"
				+ "                </table>\r\n"
				+ "            </td>\r\n"
				+ "        </tr>\r\n"
				+ "    </table>\r\n"
				+ "    <!--/100% body table-->\r\n"
				+ "</body>\r\n"
				+ "\r\n"
				+ "</html>\r\n"
				+ "", emailDetails.get("HTML").toString());
		assertEquals("SEMOSS - Database Access Request", emailDetails.get("Subject").toString());

		// Go back to the default user
		ApiSemossTestUserUtils.setDefaultTestUser();
		
		// Get Requests
		String pc3 = ApiSemossTestUtils.buildPixelCall(GetEngineUserAccessRequestReactor.class, "engine", engineId);
		NounMetadata engineRequestNM = ApiSemossTestUtils.processPixel(pc3);
		List<Map<String, Object>> requestOutput = (List<Map<String, Object>>) engineRequestNM.getValue();
		List<Map<String, String>> requests = new ArrayList<>();
		// have to tweak the map a little to send back to src code to mock front end behavior
		for (Map<String, Object> m : requestOutput) {
			Map<String, String> newMap = new HashMap<>();
			for (String s : m.keySet()) {
				Object o = m.get(s);
				String replaced = null;
				if (o != null) {
					replaced = o.toString();
				}
				if (s.equalsIgnoreCase("permission")) {
					replaced = AccessPermissionEnum.getPermissionValueById(m.get(s).toString());
				}
				newMap.put(s.toLowerCase(), replaced);
				newMap.put("userid", "user2");
			}
			requests.add(newMap);
		}
		// approve request
		String endDate = ZonedDateTime.now(ZoneId.systemDefault()).plusDays(1).toString();
		
		try {
			// this is done through monolith, adding here for the check
			SecurityEngineUtils.approveEngineUserAccessRequests(ApiSemossTestUserUtils.getUser(), engineId, requests, endDate);
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			fail("Could not approve engine access");
		}

		// verify email approved
		emails = ApiSemossTestEmailUtils.getAllEmails();
		assertEquals(2, emails.size());
		email = emails.get(0);
		assertEquals("user2@user2.com", ((List<Map<String, Object>>) email.get("To")).get(0).get("Address"));
		assertEquals("semossemailtest@gmail.com", ((Map<String, Object>) email.get("From")).get("Address"));
		emailDetails = ApiSemossTestEmailUtils.getEmail(email.get("ID").toString());
		assertEquals("<!doctype html>\r\n"
				+ "<html lang=\"en-US\">\r\n"
				+ "\r\n"
				+ "<head>\r\n"
				+ "    <meta content=\"text/html; charset=utf-8\" http-equiv=\"Content-Type\" />\r\n"
				+ "    <title>Engine Access Request Decision</title>\r\n"
				+ "    <meta name=\"description\" content=\"Insight Access Request Decision\">\r\n"
				+ "    <style type=\"text/css\">\r\n"
				+ "        a:hover {text-decoration: underline !important;}\r\n"
				+ "    </style>\r\n"
				+ "</head>\r\n"
				+ "\r\n"
				+ "<body marginheight=\"0\" topmargin=\"0\" marginwidth=\"0\" style=\"margin: 0px; background-color: #f2f3f8;\" leftmargin=\"0\">\r\n"
				+ "    <!--100% body table-->\r\n"
				+ "    <table cellspacing=\"0\" border=\"0\" cellpadding=\"0\" width=\"100%\" bgcolor=\"#f2f3f8\"\r\n"
				+ "        style=\"@import url(https://fonts.googleapis.com/css?family=Rubik:300,400,500,700|Open+Sans:300,400,600,700); font-family: 'Open Sans', sans-serif;\">\r\n"
				+ "        <tr>\r\n"
				+ "            <td>\r\n"
				+ "                <table style=\"background-color: #f2f3f8; max-width:670px;  margin:0 auto;\" width=\"100%\" border=\"0\"\r\n"
				+ "                    align=\"center\" cellpadding=\"0\" cellspacing=\"0\">\r\n"
				+ "                    <tr>\r\n"
				+ "                        <td style=\"height:80px;\">&nbsp;</td>\r\n"
				+ "                    </tr>\r\n"
				+ "                    <tr>\r\n"
				+ "                        <td style=\"height:20px;\">&nbsp;</td>\r\n"
				+ "                    </tr>\r\n"
				+ "                    <tr>\r\n"
				+ "                        <td>\r\n"
				+ "                            <table width=\"95%\" border=\"0\" align=\"center\" cellpadding=\"0\" cellspacing=\"0\"\r\n"
				+ "                                style=\"max-width:670px;background:#fff; border-radius:3px; text-align:center;-webkit-box-shadow:0 6px 18px 0 rgba(0,0,0,.06);-moz-box-shadow:0 6px 18px 0 rgba(0,0,0,.06);box-shadow:0 6px 18px 0 rgba(0,0,0,.06);\">\r\n"
				+ "                                <tr>\r\n"
				+ "                                    <td style=\"height:40px;\">&nbsp;</td>\r\n"
				+ "                                </tr>\r\n"
				+ "                                <tr>\r\n"
				+ "                                    <td style=\"padding:0 35px;\">\r\n"
				+ "                                        <h1 style=\"color:#1e1e2d; font-weight:500; margin:0;font-size:32px;font-family:'Rubik',sans-serif;\">Insight Access Request Decision</h1>\r\n"
				+ "                                        <span\r\n"
				+ "                                            style=\"display:inline-block; vertical-align:middle; margin:29px 0 26px; border-bottom:1px solid #cecece; width:100px;\"></span>\r\n"
				+ "                                        <p style=\"color:#455056; font-size:15px;line-height:24px; margin:0;\">\r\n"
				+ "                                            You have been <strong>APPROVED</strong> for the Engine: <strong>testApproveEngineRequestDB</strong><br><br>\r\n"
				+ "                                        </p>\r\n"
				+ "                                    </td>\r\n"
				+ "                                </tr>\r\n"
				+ "                                <tr>\r\n"
				+ "                                    <td style=\"height:40px;\">&nbsp;</td>\r\n"
				+ "                                </tr>\r\n"
				+ "                            </table>\r\n"
				+ "                        </td>\r\n"
				+ "                    </tr>\r\n"
				+ "                    <tr>\r\n"
				+ "                        <td style=\"height:20px;\">&nbsp;</td>\r\n"
				+ "                    </tr>\r\n"
				+ "                </table>\r\n"
				+ "            </td>\r\n"
				+ "        </tr>\r\n"
				+ "    </table>\r\n"
				+ "    <!--/100% body table-->\r\n"
				+ "</body>\r\n"
				+ "\r\n"
				+ "</html>\r\n"
				+ "", emailDetails.get("HTML").toString());
		assertEquals("SEMOSS - Engine Access Request Decision", emailDetails.get("Subject").toString());
		
		// verify permission (User has edit only permission so switch back to two and check to see if you can access requests)
		ApiSemossTestUserUtils.setUser("user2");
		String pc4 = ApiSemossTestUtils.buildPixelCall(GetEngineUserAccessRequestReactor.class, "engine", engineId);
		ApiSemossTestUtils.processPixel(pc4);
		
//		// verify audit history
//		String pc5 = ApiSemossTestUtils.buildPixelCall(EnginePermissionHistoryReactor.class, "engine", engineId);
//		NounMetadata engineHistoryNM = ApiSemossTestUtils.processPixel(pc5);
//		List<Map<String, Object>> engineHistory = (List<Map<String, Object>>) engineHistoryNM.getValue();
//		assertEquals(2, engineHistory.size());
//		
//		Map<String, Object> decision = engineHistory.get(0);
//		assertEquals("user2", decision.get("REQUEST_USERID"));
//		assertEquals("Native", decision.get("REQUEST_TYPE"));
//		assertEquals(engineId, decision.get("OBJECTID"));
//		assertEquals("ENGINE", decision.get("OBJECTTYPE"));
//		assertNull(decision.get("PROJECTID"));
//		assertEquals(2, decision.get("PERMISSION"));
//		assertEquals("this is a test request", decision.get("REQUEST_REASON"));
//		assertEquals("ater", decision.get("APPROVER_USERID"));
//		assertEquals("NATIVE", decision.get("APPROVER_TYPE"));
//		assertEquals("APPROVED", decision.get("APPROVER_DECISION"));
//		assertNotNull(decision.get("APPROVER_TIMESTAMP"));
//		assertNotNull(decision.get("ENDDATE"));
//		assertEquals("user2", decision.get("SUBMITTED_BY_USERID"));
//		assertEquals("NATIVE", decision.get("SUBMITTED_BY_TYPE"));
//		
//		Map<String, Object> request = engineHistory.get(1);
//		assertEquals("user2", request.get("REQUEST_USERID"));
//		assertEquals("Native", request.get("REQUEST_TYPE"));
//		assertEquals(engineId, request.get("OBJECTID"));
//		assertEquals("ENGINE", request.get("OBJECTTYPE"));
//		assertNull(request.get("PROJECTID"));
//		assertEquals(2, request.get("PERMISSION"));
//		assertEquals("this is a test request", request.get("REQUEST_REASON"));
//		assertNull(request.get("APPROVER_USERID"));
//		assertNull(request.get("APPROVER_TYPE"));
//		assertEquals("NEW_REQUEST", request.get("APPROVER_DECISION"));
//		assertNull(request.get("APPROVER_TIMESTAMP"));
//		assertNull(request.get("ENDDATE"));
//		assertEquals("user2", request.get("SUBMITTED_BY_USERID"));
//		assertEquals("NATIVE", request.get("SUBMITTED_BY_TYPE"));
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testDeny() {
		// make engine get engine id
		Map<String, String> additionalDataTypes = new HashMap<>();
		additionalDataTypes.put("datesubmitted", "yyyy-MM-d H:mm:ss");

		String engineId = ApiSemossTestEngineUtils.addTestRdbmsDatabase(
				"testDenyEngineRequestDB",
				Arrays.asList("yes", "no", "maybeso", "datesubmitted"),
				Arrays.asList("INT", "INT", "INT", "TIMESTAMP"),
				additionalDataTypes,
				Arrays.asList(Arrays.asList("1", "0", "0", "2022-09-13 12:00:01"))
				);
	
		// No reactor, so just use Utility class in src code.
		// Other option is to make a Http Request, which is kind of hard
		try {
			SecurityEngineUtils.setEngineDiscoverable(ApiSemossTestUserUtils.getUser(), engineId, true);
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail("could not set engine discoverable");
		}

		// make another user
		ApiSemossTestUserUtils.addAndSetNewNativeUser("user2", false);
		
		// request access from second user to engine
		String pc2 = ApiSemossTestUtils.buildPixelCall(RequestEngineReactor.class, "engine", engineId, "permission", "2", "comment", "this is a test request");
		ApiSemossTestUtils.processPixel(pc2);
		
		// verify email sent
		List<Map<String, Object>> emails = ApiSemossTestEmailUtils.getAllEmails();
		assertEquals(1, emails.size());
		Map<String, Object> email = emails.get(0);
		assertEquals("ater@ater.com", ((List<Map<String, Object>>) email.get("To")).get(0).get("Address"));
		assertEquals("semossemailtest@gmail.com", ((Map<String, Object>) email.get("From")).get("Address"));
		
		Map<String, Object> emailDetails = ApiSemossTestEmailUtils.getEmail(email.get("ID").toString());
		assertEquals("<!doctype html>\r\n"
				+ "<html lang=\"en-US\">\r\n"
				+ "\r\n"
				+ "<head>\r\n"
				+ "    <meta content=\"text/html; charset=utf-8\" http-equiv=\"Content-Type\" />\r\n"
				+ "    <title>Engine Access Request</title>\r\n"
				+ "    <meta name=\"description\" content=\"Engine Access Request\">\r\n"
				+ "    <style type=\"text/css\">\r\n"
				+ "        a:hover {text-decoration: underline !important;}\r\n"
				+ "    </style>\r\n"
				+ "</head>\r\n"
				+ "\r\n"
				+ "<body marginheight=\"0\" topmargin=\"0\" marginwidth=\"0\" style=\"margin: 0px; background-color: #f2f3f8;\" leftmargin=\"0\">\r\n"
				+ "    <!--100% body table-->\r\n"
				+ "    <table cellspacing=\"0\" border=\"0\" cellpadding=\"0\" width=\"100%\" bgcolor=\"#f2f3f8\"\r\n"
				+ "        style=\"@import url(https://fonts.googleapis.com/css?family=Rubik:300,400,500,700|Open+Sans:300,400,600,700); font-family: 'Open Sans', sans-serif;\">\r\n"
				+ "        <tr>\r\n"
				+ "            <td>\r\n"
				+ "                <table style=\"background-color: #f2f3f8; max-width:670px;  margin:0 auto;\" width=\"100%\" border=\"0\"\r\n"
				+ "                    align=\"center\" cellpadding=\"0\" cellspacing=\"0\">\r\n"
				+ "                    <tr>\r\n"
				+ "                        <td style=\"height:80px;\">&nbsp;</td>\r\n"
				+ "                    </tr>\r\n"
				+ "                    <tr>\r\n"
				+ "                        <td style=\"height:20px;\">&nbsp;</td>\r\n"
				+ "                    </tr>\r\n"
				+ "                    <tr>\r\n"
				+ "                        <td>\r\n"
				+ "                            <table width=\"95%\" border=\"0\" align=\"center\" cellpadding=\"0\" cellspacing=\"0\"\r\n"
				+ "                                style=\"max-width:670px;background:#fff; border-radius:3px; text-align:center;-webkit-box-shadow:0 6px 18px 0 rgba(0,0,0,.06);-moz-box-shadow:0 6px 18px 0 rgba(0,0,0,.06);box-shadow:0 6px 18px 0 rgba(0,0,0,.06);\">\r\n"
				+ "                                <tr>\r\n"
				+ "                                    <td style=\"height:40px;\">&nbsp;</td>\r\n"
				+ "                                </tr>\r\n"
				+ "                                <tr>\r\n"
				+ "                                    <td style=\"padding:0 35px;\">\r\n"
				+ "                                        <h1 style=\"color:#1e1e2d; font-weight:500; margin:0;font-size:32px;font-family:'Rubik',sans-serif;\">Engine Access Request</h1>\r\n"
				+ "                                        <span\r\n"
				+ "                                            style=\"display:inline-block; vertical-align:middle; margin:29px 0 26px; border-bottom:1px solid #cecece; width:100px;\"></span>\r\n"
				+ "                                        <p style=\"color:#455056; font-size:15px;line-height:24px; margin:0;\">\r\n"
				+ "                                            The following user has requested access for your Engine: <strong>testDenyEngineRequestDB</strong><br><br>\r\n"
				+ "                                            <strong>Username:</strong> <br>\r\n"
				+ "                                            <strong>Email:</strong> user2@user2.com<br>\r\n"
				+ "                                            <strong>Permission:</strong> EDIT<br>\r\n"
				+ "                                            <strong>Reason: </strong>this is a test request\r\n"
				+ "                                        </p>\r\n"
				+ "                                    </td>\r\n"
				+ "                                </tr>\r\n"
				+ "                                <tr>\r\n"
				+ "                                    <td style=\"height:40px;\">&nbsp;</td>\r\n"
				+ "                                </tr>\r\n"
				+ "                            </table>\r\n"
				+ "                        </td>\r\n"
				+ "                    </tr>\r\n"
				+ "                    <tr>\r\n"
				+ "                        <td style=\"height:20px;\">&nbsp;</td>\r\n"
				+ "                    </tr>\r\n"
				+ "                </table>\r\n"
				+ "            </td>\r\n"
				+ "        </tr>\r\n"
				+ "    </table>\r\n"
				+ "    <!--/100% body table-->\r\n"
				+ "</body>\r\n"
				+ "\r\n"
				+ "</html>\r\n"
				+ "", emailDetails.get("HTML").toString());
		assertEquals("SEMOSS - Database Access Request", emailDetails.get("Subject").toString());

		// Go back to the default user
		ApiSemossTestUserUtils.setDefaultTestUser();
		
		// Get Requests
		String pc3 = ApiSemossTestUtils.buildPixelCall(GetEngineUserAccessRequestReactor.class, "engine", engineId);
		NounMetadata engineRequestNM = ApiSemossTestUtils.processPixel(pc3);
		List<Map<String, Object>> requestOutput = (List<Map<String, Object>>) engineRequestNM.getValue();
		List<String> requestStrings = requestOutput.stream().map(s -> s.get("ID").toString()).collect(Collectors.toList());
		
		try {
			// this is done through monolith, adding here for the check
			SecurityEngineUtils.denyEngineUserAccessRequests(ApiSemossTestUserUtils.getUser(), engineId, requestStrings);
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			fail("Could not approve engine access");
		}

		// verify email approved
		emails = ApiSemossTestEmailUtils.getAllEmails();
		assertEquals(2, emails.size());
		email = emails.get(0);
		assertEquals("user2@user2.com", ((List<Map<String, Object>>) email.get("To")).get(0).get("Address"));
		assertEquals("semossemailtest@gmail.com", ((Map<String, Object>) email.get("From")).get("Address"));
		emailDetails = ApiSemossTestEmailUtils.getEmail(email.get("ID").toString());
		assertEquals("<!doctype html>\r\n"
				+ "<html lang=\"en-US\">\r\n"
				+ "\r\n"
				+ "<head>\r\n"
				+ "    <meta content=\"text/html; charset=utf-8\" http-equiv=\"Content-Type\" />\r\n"
				+ "    <title>Engine Access Request Decision</title>\r\n"
				+ "    <meta name=\"description\" content=\"Insight Access Request Decision\">\r\n"
				+ "    <style type=\"text/css\">\r\n"
				+ "        a:hover {text-decoration: underline !important;}\r\n"
				+ "    </style>\r\n"
				+ "</head>\r\n"
				+ "\r\n"
				+ "<body marginheight=\"0\" topmargin=\"0\" marginwidth=\"0\" style=\"margin: 0px; background-color: #f2f3f8;\" leftmargin=\"0\">\r\n"
				+ "    <!--100% body table-->\r\n"
				+ "    <table cellspacing=\"0\" border=\"0\" cellpadding=\"0\" width=\"100%\" bgcolor=\"#f2f3f8\"\r\n"
				+ "        style=\"@import url(https://fonts.googleapis.com/css?family=Rubik:300,400,500,700|Open+Sans:300,400,600,700); font-family: 'Open Sans', sans-serif;\">\r\n"
				+ "        <tr>\r\n"
				+ "            <td>\r\n"
				+ "                <table style=\"background-color: #f2f3f8; max-width:670px;  margin:0 auto;\" width=\"100%\" border=\"0\"\r\n"
				+ "                    align=\"center\" cellpadding=\"0\" cellspacing=\"0\">\r\n"
				+ "                    <tr>\r\n"
				+ "                        <td style=\"height:80px;\">&nbsp;</td>\r\n"
				+ "                    </tr>\r\n"
				+ "                    <tr>\r\n"
				+ "                        <td style=\"height:20px;\">&nbsp;</td>\r\n"
				+ "                    </tr>\r\n"
				+ "                    <tr>\r\n"
				+ "                        <td>\r\n"
				+ "                            <table width=\"95%\" border=\"0\" align=\"center\" cellpadding=\"0\" cellspacing=\"0\"\r\n"
				+ "                                style=\"max-width:670px;background:#fff; border-radius:3px; text-align:center;-webkit-box-shadow:0 6px 18px 0 rgba(0,0,0,.06);-moz-box-shadow:0 6px 18px 0 rgba(0,0,0,.06);box-shadow:0 6px 18px 0 rgba(0,0,0,.06);\">\r\n"
				+ "                                <tr>\r\n"
				+ "                                    <td style=\"height:40px;\">&nbsp;</td>\r\n"
				+ "                                </tr>\r\n"
				+ "                                <tr>\r\n"
				+ "                                    <td style=\"padding:0 35px;\">\r\n"
				+ "                                        <h1 style=\"color:#1e1e2d; font-weight:500; margin:0;font-size:32px;font-family:'Rubik',sans-serif;\">Insight Access Request Decision</h1>\r\n"
				+ "                                        <span\r\n"
				+ "                                            style=\"display:inline-block; vertical-align:middle; margin:29px 0 26px; border-bottom:1px solid #cecece; width:100px;\"></span>\r\n"
				+ "                                        <p style=\"color:#455056; font-size:15px;line-height:24px; margin:0;\">\r\n"
				+ "                                            You have been <strong>DENIED</strong> for the Engine: <strong>testDenyEngineRequestDB</strong><br><br>\r\n"
				+ "                                        </p>\r\n"
				+ "                                    </td>\r\n"
				+ "                                </tr>\r\n"
				+ "                                <tr>\r\n"
				+ "                                    <td style=\"height:40px;\">&nbsp;</td>\r\n"
				+ "                                </tr>\r\n"
				+ "                            </table>\r\n"
				+ "                        </td>\r\n"
				+ "                    </tr>\r\n"
				+ "                    <tr>\r\n"
				+ "                        <td style=\"height:20px;\">&nbsp;</td>\r\n"
				+ "                    </tr>\r\n"
				+ "                </table>\r\n"
				+ "            </td>\r\n"
				+ "        </tr>\r\n"
				+ "    </table>\r\n"
				+ "    <!--/100% body table-->\r\n"
				+ "</body>\r\n"
				+ "\r\n"
				+ "</html>\r\n"
				+ "", emailDetails.get("HTML").toString());
		assertEquals("SEMOSS - Engine Access Request Decision", emailDetails.get("Subject").toString());
		
		// verify permission (User has edit only permission so switch back to two and check to see if you can access requests)
		ApiSemossTestUserUtils.setUser("user2");
		String pc4 = ApiSemossTestUtils.buildPixelCall(GetEngineUserAccessRequestReactor.class, "engine", engineId);
		NounMetadata erroredNM = ApiSemossTestUtils.processRawPixel(pc4);
		ApiSemossTestUtils.checkNounMetadataError(erroredNM, "User does not have permission to view access requests for this engine");
		
		// verify audit history
//		ApiSemossTestUserUtils.setDefaultTestUser();
//		String pc5 = ApiSemossTestUtils.buildPixelCall(EnginePermissionHistoryReactor.class, "engine", engineId);
//		NounMetadata engineHistoryNM = ApiSemossTestUtils.processPixel(pc5);
//		List<Map<String, Object>> engineHistory = (List<Map<String, Object>>) engineHistoryNM.getValue();
//		assertEquals(2, engineHistory.size());
//		
//		Map<String, Object> decision = engineHistory.get(0);
//		assertEquals("user2", decision.get("REQUEST_USERID"));
//		assertEquals("Native", decision.get("REQUEST_TYPE"));
//		assertEquals(engineId, decision.get("OBJECTID"));
//		assertEquals("ENGINE", decision.get("OBJECTTYPE"));
//		assertNull(decision.get("PROJECTID"));
//		assertEquals(2, decision.get("PERMISSION"));
//		assertEquals("this is a test request", decision.get("REQUEST_REASON"));
//		assertEquals("ater", decision.get("APPROVER_USERID"));
//		assertEquals("Native", decision.get("APPROVER_TYPE"));
//		assertEquals("DENIED", decision.get("APPROVER_DECISION"));
//		assertNotNull(decision.get("APPROVER_TIMESTAMP"));
//		assertNull(decision.get("ENDDATE"));
//		assertEquals("user2", decision.get("SUBMITTED_BY_USERID"));
//		assertEquals("NATIVE", decision.get("SUBMITTED_BY_TYPE"));
//		
//		Map<String, Object> request = engineHistory.get(1);
//		assertEquals("user2", request.get("REQUEST_USERID"));
//		assertEquals("Native", request.get("REQUEST_TYPE"));
//		assertEquals(engineId, request.get("OBJECTID"));
//		assertEquals("ENGINE", request.get("OBJECTTYPE"));
//		assertNull(request.get("PROJECTID"));
//		assertEquals(2, request.get("PERMISSION"));
//		assertEquals("this is a test request", request.get("REQUEST_REASON"));
//		assertNull(request.get("APPROVER_USERID"));
//		assertNull(request.get("APPROVER_TYPE"));
//		assertEquals("NEW_REQUEST", request.get("APPROVER_DECISION"));
//		assertNull(request.get("APPROVER_TIMESTAMP"));
//		assertNull(request.get("ENDDATE"));
//		assertEquals("user2", request.get("SUBMITTED_BY_USERID"));
//		assertEquals("NATIVE", request.get("SUBMITTED_BY_TYPE"));
	}
	
}
