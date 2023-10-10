package prerna.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;

import prerna.tcp.PayloadStruct;
import prerna.util.Utility;

public class NativePySockTest implements Runnable {

	InputStream is = null;
	OutputStream os = null;
	int epocCounter = 0;
	Gson gson = new Gson();
	boolean connected = true;
	BufferedReader br = null;


	public static void main(String[] args) { // throws UnknownHostException, IOException {
		// TODO Auto-generated method stub
	
		NativePySockTest test = new NativePySockTest();
		
		test.bufTest();
		
		test.castTest();
		test.connect();
		test.initReader();
		Thread t = new Thread(test);
		t.start();
		test.execCommands();
		test.write();
		//test.shutdown();
		
	}
	
	public void execCommands()
	{
		try {
			PayloadStruct ps = new PayloadStruct();
			ps.operation = ps.operation.CMD;
			ps.payload = new Object[] {"temp", "c:/temp"};
			ps.methodName = "constructor";
			ps.hasReturn = false;
			
			ps.epoc = "CMD1";
			ps.insightId = "temp__c:/temp";
			byte[] finalByte = pack(gson.toJson(ps), ps.epoc);
			os.write(finalByte);

			//Thread.sleep(3000);
			
			ps = new PayloadStruct();
			ps.operation = ps.operation.CMD;
			ps.payload = new Object[] {"dir"};
			ps.methodName = "executeCommand";
			ps.hasReturn = false;
			
			ps.epoc = "CMD2";
			ps.insightId = "temp__c:/temp";
			finalByte = pack(gson.toJson(ps), ps.epoc);
			os.write(finalByte);

			ps = new PayloadStruct();
			ps.operation = ps.operation.CMD;
			ps.payload = new Object[] {"cd try1"};
			ps.methodName = "executeCommand";
			ps.hasReturn = false;
			//Thread.sleep(3000);
			
			ps.epoc = "CMD3";
			ps.insightId = "temp__c:/temp";
			finalByte = pack(gson.toJson(ps), ps.epoc);
			os.write(finalByte);

			ps = new PayloadStruct();
			ps.operation = ps.operation.CMD;
			ps.payload = new Object[] {"dir"};
			ps.methodName = "executeCommand";
			ps.hasReturn = false;			
			ps.epoc = "CMD4";
			ps.insightId = "temp__c:/temp";
			finalByte = pack(gson.toJson(ps), ps.epoc);
			os.write(finalByte);
			
			ps = new PayloadStruct();
			ps.operation = ps.operation.CMD;
			ps.payload = new Object[] {"git   init"};
			ps.methodName = "executeCommand";
			ps.hasReturn = false;			
			ps.epoc = "CMD4";
			ps.insightId = "temp__c:/temp";
			finalByte = pack(gson.toJson(ps), ps.epoc);
			os.write(finalByte);

			ps = new PayloadStruct();
			ps.operation = ps.operation.CMD;
			ps.payload = new Object[] {"cd.."};
			ps.methodName = "executeCommand";
			ps.hasReturn = false;			
			ps.epoc = "CMD4";
			ps.insightId = "temp__c:/temp";
			finalByte = pack(gson.toJson(ps), ps.epoc);
			os.write(finalByte);

			ps = new PayloadStruct();
			ps.operation = ps.operation.CMD;
			ps.payload = new Object[] {"deltree try1"};
			ps.methodName = "executeCommand";
			ps.hasReturn = false;			
			ps.epoc = "CMD4";
			ps.insightId = "temp__c:/temp";
			finalByte = pack(gson.toJson(ps), ps.epoc);
			os.write(finalByte);
			
			System.err.println("stopper.. ");
			//PayloadStruct retPS = (PayloadStruct)tcpClient.executeCommand(ps);			

			
//		PayloadStruct ps = new PayloadStruct();
//		ps.operation = ps.operation.CMD;
//		ps.payload = new Object[] {command};
//		ps.methodName = "executeCommand";
//		ps.insightId = mountName + "__" + mountDir;
//		ps.payloadClasses = new Class[] {String.class};
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch(Exception ignored)
		{
			
		}

	}
	
	
	public void bufTest()
	{
		// tests byte buffers
		String input = "ps0000000000001";
		ByteBuffer bytes = ByteBuffer.allocate(4).wrap(input.getBytes(StandardCharsets.UTF_8));
		
		byte [] arr = bytes.array();
		
		System.out.println("Length " + arr.length);
		
		byte [] output = new byte[4];
		bytes.get(output);
		
		System.err.println(new String(arr));
		
	}
	
	public void pickleTest()
	{
		String input = "Jo Mama";
		
	}
	
	public void castTest()
	{
		String sample = "abc\nxyz";
		System.err.println(sample);
		sample = sample.replace("\n", ";");
		System.err.println(sample);
		
		try {
			Class clz = Class.forName("java.lang.String");
			System.err.println(clz);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Object obj = this.gson.fromJson("True", Object.class);
		System.err.println(obj);
		System.err.println(obj.getClass());
		String trialCommand = "{'a':1, 'insightId': 'TempInsight_fca2c035-6d85-4025-af8f-59656450b951', 'user': {'port': '9999', 'workspaceProjectMap': {}, 'assetProjectMap': {}, 'primaryLogin': 'NATIVE', 'accessTokens': {'Native': {'provider': 'NATIVE', 'userGroups': [], 'id': 'prabhuk12@gmail.com', 'expires_in': 0, 'token_type': 'Bearer', 'startTime': -1, 'email': 'prabhuk12@gmail.com', 'name': 'prabhuk', 'sans': {}, 'locked': False, 'lastLogin': {'strDate': '2023-07-02 12:09:15.385', 'pattern': 'yyyy-MM-dd HH:mm:ss.S'}}}, 'loggedInProfiles': ['NATIVE'], 'insightSecret': {}, 'sharedSessions': [], 'projectIdMap': {'eed12b32-bc38-4718-ab73-c0c78480c174': 'TAP_Site_Data', '31a4a3c7-2855-4fa7-8d55-3a96dfca9a7e': 'CPCE_Insights', '98d7ec5d-3e71-42c0-9fa5-f32e903c9134': 'Food', '92b0083f-0c22-4530-b2b8-4439a85d4e21': 'Mv_Audit', '3a7bf640-9129-4515-ab5e-6a54272743d7': 'Asset', '9f78e44f-64cc-456a-925f-b5062783315f': 'FrameTester', '0ee98941-3e27-472f-805e-50dbfc1b39a0': 'ASAALT', '75282231-f7b6-4f95-8be5-2099a32c9e94': 'EDAF Standards', '4c7c8c55-840f-47c8-b41a-a57e5b8dd84b': 'REngine', 'b7ef29ce-92b3-4720-aece-f626ac48c424': 'Diabetes2', '1e4c33f9-a7b7-48ed-aa89-e1546591f5bf': 'GitTest2', 'df7be6f8-9ec3-4707-9d68-bf91141281b9': 'DECOM', '820972d8-1df1-49f3-869d-9a12517e7ac7': 'Full PITM', '4ab3fdd3-898c-4115-9b96-e925efe7baa9': 'GitTest', '60d03e3c-5e50-46df-a7bb-12b2a51eac89': 'Diabetes Blood Glucose', '6981102e-215f-4613-94a3-91be40cf1de7': 'Patient Predict', 'cd170b5c-47c7-4c75-94c7-d8428cb4f695': 'Standards Pedigree', 'COUNT': '50', 'bc85f8ad-3dfc-48c3-8dd2-477021d2a61c': 'Navy Demo', '01d85d8d-5827-455e-898d-4ea5c30c71f8': 'Go Live Date', '7c7c8c55-840f-47c8-b41a-a57e5b8dd84b': 'Suppliers', 'fcee8e59-bb1d-4d0e-815d-0de8d265ddaf': 'IndependenceData', '049b3b6a-6630-4f77-8f3a-2892fec2188b': 'Blood Glucose', '6adbe3cc-ac05-454d-8d31-cb7df29c20b2': 'All Standards', '8879489f-0303-4e19-b12f-9fc0a0d54e82': 'D2D Market Info', '15f480f7-99c0-4b54-8ba6-e7080adb030d': 'Informatica', 'bd47b02f-49d8-469d-b0ec-de0c900ef652': 'Mango', '80e11662-2fe5-4597-9245-4b245e58a04d': 'Army_Demo', '0890ef56-d45b-4841-94f3-7d38e217b2fc': 'Attachment 12 - CHCS Data', '68959667-23d5-452c-b222-75687318a3f4': 'PITM PISM', 'cb8c6d2d-bce3-4477-ae99-5f2e08061ab2': 'Snowflake_PharmDB', 'f7ecd448-63d1-4bcb-a2a1-5c1dd85e5303': 'Temp', 'e54f4d69-be2c-456e-bd73-9ccd3d9e1e33': 'Shnowflake', '5270d6bf-52a2-4167-9301-23d6569eb782': 'Pharmacy Claims', '133db94b-4371-4763-bff9-edf7e5ed021b': 'TAP_Core_Data', 'bf47a518-1929-4ab7-9514-9ad524e0a6bc': 'Full PISM', 'aba4dcc6-1fe5-423e-8d08-1f534aa98c42': 'Pharmacy Malpractice', '6c7c8c55-840f-47c8-b41a-a57e5b8dd84b': 'Customers', '2b244110-5610-4641-b17c-cbb29181e2a3': 'NETCOM Tools', '43ae2362-cddd-4eff-83ed-d9bb2d726ca4': 'GitApp', 'ed700aed-1ba7-488f-824c-18be95367fbf': 'ATGR Linked', 'b210b43b-4272-43d6-9429-feacb4e7c9d9': 'BEA Database', '30991037-1e73-49f5-99d3-f28210e6b95c': 'DISR', 'b6fe5758-e69e-43a5-9918-0b61e3b4a6e0': 'Actor3', '854544eb-6488-4e24-b0ba-9f6f903be4f7': 'Fed_Head_Count', '80ffbf6e-ae31-4da3-92a3-0c17761e87d5': 'SDD System Contracts', 'b98c5d74-c410-4f83-93fa-939b4eccb8f0': 'GitApp2', '2179c054-262f-4e54-8aa3-4adb13be0911': 'APMS', 'aef11195-f757-4b01-8d12-724c85a65640': 'System Stage Tracker', '275d3315-039c-47e6-8c40-88bfda54c487': 'NDC Data Set', '5c7c8c55-840f-47c8-b41a-a57e5b8dd84b': 'Alliance'}, 'engineIdMap': {}, 'varMap': {}, 'anonymous': False, 'rPort': -1, 'pyPort': 9999, 'forcePort': -1, 'insightSerializedMap': {}}, 'cacheable': True, 'cacheMinutes': -1, 'cacheEncrypt': False, 'count': 0, 'isOldInsight': False, 'pragmap': {'xCache': 'false'}, 'baseURL': 'http://localhost:8080/appui/#!/', 'contextReinitialized': False, 'sqlWrapperMap': {}, 'id2SQLMapper': {}, 'idCount': 0}";
		obj = this.gson.fromJson(trialCommand, Object.class);
		System.err.println(obj);
		String command2 = "{'index': [0, 1, 11, 10, 8, 7, 9, 5, 4, 3, 2, 6, 12, 13, 14, 15, 16, 17, 21, 20], 'columns': ['AGE', 'BP_1D', 'BP_1S', 'BP_2D', 'BP_2S', 'CHOL', 'DIABETES_UNIQUE_ROW_ID', 'DRUG', 'END_DATE', 'FRAME', 'GENDER', 'GLYHB', 'HDL', 'HEIGHT', 'HIP', 'ID', 'LOCATION', 'RATIO', 'SEMOSS_EXPORT_20221024_233136_UNIQUE_ROW_ID', 'STAB_GLU', 'START_DATE', 'TIME_PPN', 'WAIST', 'WEIGHT'], 'data': [[19.0, 58.0, 108.0, nan, nan, 146.0, 251.0, 'Lantus,_bydureon,_Symlin', 2, 'medium', 'female', 4.760000229, 41.0, 60.0, 40.0, 17790.0, 'Buckingham', 3.599999905, 1.0, 79.0, Timestamp('2013-08-28 00:00:00'), 240.0, 33.0, 135.0], [19.0, 70.0, 118.0, nan, nan, 193.0, 115.0, 'Lantus,_bydureon,_Symlin,_Humalog,_humulin', Timestamp('2014-10-05 00:00:00'), 'small', 'female', 4.309999943, 49.0, 61.0, 38.0, 4823.0, 'Louisa', 3.900000095, 2.0, 77.0, Timestamp('2013-10-26 00:00:00'), 300.0, 32.0, 119.0], [20.0, 110.0, 165.0, 100.0, 153.0, 193.0, 56.0, 'Tradjenta,_Lantus,_bydureon,_Symlin,_Humalog', Timestamp('2014-03-13 00:00:00'), 'small', 'female', 6.349999905, 63.0, 68.0, 58.0, 2763.0, 'Buckingham', 3.099999905, 12.0, 106.0, Timestamp('2013-11-21 00:00:00'), 60.0, 49.0, 274.0], [20.0, 100.0, 140.0, 82.0, 138.0, 179.0, 374.0, 'Tradjenta,_Invokana,_Onglyza,Lantus', Timestamp('2014-06-06 00:00:00'), 'medium', 'female', 4.679999828, 60.0, 58.0, 46.0, 40804.0, 'Louisa', 3.0, 11.0, 105.0, Timestamp('2013-10-20 00:00:00'), 270.0, 34.0, 170.0], [20.0, 86.0, 132.0, nan, nan, 174.0, 231.0, 'Invokana,_Onglyza,Humalog,_humulin', Timestamp('2014-06-09 00:00:00'), 'medium', 'male', 5.53000021, 117.0, 70.0, 41.0, 16005.0, 'Buckingham', 1.5, 9.0, 105.0, Timestamp('2013-08-13 00:00:00'), 210.0, 37.0, 187.0], [20.0, 86.0, 122.0, nan, nan, 164.0, 307.0, 'bydureon,_Symlin,_Humalog', Timestamp('2014-09-21 00:00:00'), 'small', 'female', 3.970000029, 67.0, 70.0, 39.0, 20350.0, 'Louisa', 2.400000095, 8.0, 91.0, Timestamp('2013-05-09 00:00:00'), 390.0, 32.0, 141.0], [20.0, 90.0, 100.0, nan, nan, 230.0, 18.0, 'Symlin,_Humalog,_humulin', Timestamp('2014-07-13 00:00:00'), 'med', 'male', 4.53000021, 64.0, 67.0, 39.0, 1041.0, 'Louisa', 3.599999905, 10.0, 112.0, Timestamp('2013-07-19 00:00:00'), 1440.0, 31.0, 159.0], [20.0, 78.0, 108.0, nan, nan, 164.0, 150.0, 'Tradjenta,_Invokana,_Onglyza', Timestamp('2014-09-08 00:00:00'), 'small', 'male', 4.510000229, 63.0, 72.0, 36.0, 12778.0, 'Buckingham', 2.599999905, 6.0, 71.0, Timestamp('2013-12-19 00:00:00'), 1080.0, 29.0, 145.0], [20.0, 72.0, 110.0, nan, nan, 217.0, 366.0, 'Tradjenta,_Invokana,_Onglyza,Lantus', Timestamp('2014-10-18 00:00:00'), 'medium', 'female', 3.660000086, 54.0, 67.0, 45.0, 40785.0, 'Louisa', 4.0, 5.0, 75.0, Timestamp('2013-03-27 00:00:00'), 1440.0, 40.0, 187.0], [20.0, 70.0, 108.0, nan, nan, 170.0, 73.0, 'Tradjenta,_Lantus,_bydureon,_Symlin,_Humalog', Timestamp('2014-03-23 00:00:00'), 'medium', 'female', 4.389999866, 64.0, 64.0, 40.0, 3750.0, 'Buckingham', 2.700000048, 4.0, 69.0, Timestamp('2013-09-28 00:00:00'), 120.0, 37.0, 161.0], [20.0, 64.0, 122.0, nan, nan, 226.0, 303.0, 'bydureon,_Symlin,_Humalog', Timestamp('2014-06-25 00:00:00'), 'small', 'female', 3.880000114, 70.0, 64.0, 39.0, 20337.0, 'Louisa', 3.200000048, 3.0, 97.0, Timestamp('2013-07-03 00:00:00'), 90.0, 31.0, 114.0], [20.0, 82.0, 105.0, nan, nan, 149.0, 255.0, 'Lantus,_bydureon,_Symlin', Timestamp('2014-11-25 00:00:00'), 'small', 'female', 4.5, 49.0, 62.0, 37.0, 17800.0, 'Buckingham', 3.0, 7.0, 77.0, Timestamp('2013-02-12 00:00:00'), 720.0, 31.0, 115.0], [21.0, 62.0, 112.0, nan, nan, 132.0, 126.0, 'Lantus,_bydureon,_Symlin,_Humalog,_humulin', Timestamp('2014-03-01 00:00:00'), 'L', 'female', 4.010000229, 34.0, 65.0, 43.0, 10001.0, 'Buckingham', 3.900000095, 13.0, 99.0, Timestamp('2013-08-05 00:00:00'), 180.0, 39.0, 169.0], [21.0, 68.0, 110.0, nan, nan, 135.0, 289.0, 'bydureon,_Symlin,_Humalog', Timestamp('2014-10-03 00:00:00'), 'small', 'male', 4.210000038, 47.0, 69.0, 39.0, 20294.0, 'Louisa', 2.900000095, 14.0, 88.0, Timestamp('2013-04-27 00:00:00'), 10.0, 31.0, 155.0], [21.0, 76.0, 116.0, nan, nan, 244.0, 314.0, 'bydureon,_Symlin,_Humalog', Timestamp('2014-04-30 00:00:00'), 'medium', 'male', 4.539999962, 92.0, 71.0, 39.0, 20369.0, 'Louisa', 2.700000048, 15.0, 89.0, Timestamp('2013-04-19 00:00:00'), 180.0, 34.0, 163.0], [21.0, 82.0, 130.0, nan, nan, 193.0, 343.0, 'Tradjenta,_Invokana,_Onglyza,Lantus', Timestamp('2014-09-06 00:00:00'), 'small', 'female', 5.010000229, 49.0, 61.0, 52.0, 21338.0, 'Louisa', 3.900000095, 16.0, 75.0, Timestamp('2013-05-03 00:00:00'), 240.0, 40.0, 220.0], [21.0, 85.0, 125.0, 68.0, 117.0, 203.0, 96.0, 'bydureon,_Symlin,_Humalog,_humulin', Timestamp('2014-08-01 00:00:00'), 'medium', 'female', 4.099999905, 75.0, 63.0, 39.0, 4783.0, 'Louisa', 2.700000048, 17.0, 84.0, Timestamp('2013-04-05 00:00:00'), 900.0, 28.0, 142.0], [21.0, 88.0, 138.0, nan, nan, 187.0, 169.0, 'Tradjenta,_Invokana,_Onglyza', Timestamp('2014-03-27 00:00:00'), 'small', 'female', 4.400000095, 64.0, 63.0, 43.0, 15264.0, 'Buckingham', 2.900000095, 18.0, 84.0, Timestamp('2013-11-01 00:00:00'), 180.0, 39.0, 158.0], [22.0, 78.0, 112.0, nan, nan, 165.0, 298.0, 'bydureon,_Symlin,_Humalog', Timestamp('2014-05-05 00:00:00'), 'small', 'female', 3.690000057, 46.0, 63.0, 35.0, 20318.0, 'Louisa', 3.599999905, 22.0, 76.0, Timestamp('2013-04-18 00:00:00'), 120.0, 28.0, 114.0], [22.0, 75.0, 120.0, nan, nan, 217.0, 79.0, 'bydureon,_Symlin,_Humalog,_humulin', Timestamp('2014-05-28 00:00:00'), 'medium', 'female', 3.930000067, 60.0, 71.0, 50.0, 4506.0, 'Buckingham', 3.599999905, 21.0, 81.0, Timestamp('2013-01-22 00:00:00'), 210.0, 46.0, 223.0]]}";
		command2 = "{\'index\': [0, 1, 11, 10, 8, 7, 9, 5, 4, 3, 2, 6, 12, 13, 14, 15, 16, 17, 21, 20], \'columns\': [\'AGE\', \'BP_1D\', \'BP_1S\', \'BP_2D\', \'BP_2S\', \'CHOL\', \'DIABETES_UNIQUE_ROW_ID\', \'DRUG\', \'END_DATE\', \'FRAME\', \'GENDER\', \'GLYHB\', \'HDL\', \'HEIGHT\', \'HIP\', \'ID\', \'(LOCATION)\', \'RATIO\', \'SEMOSS_EXPORT_20221024_233136_UNIQUE_ROW_ID\', \'STAB_GLU\', \'START_DATE\', \'TIME_PPN\', \'WAIST\', \'WEIGHT\'], \'data\': [[19.0, 58.0, 108.0, nan, nan, 146.0, 251.0, \'Lantus,_bydureon,_Symlin\', '2014-10-10 00:00:00', \'medium\', \'female\', 4.760000229, 41.0, 60.0, 40.0, 17790.0, \'Buckingham\', 3.599999905, 1.0, 79.0, '2013-08-28 00:00:00', 240.0, 33.0, 135.0]]}";
		obj = this.gson.fromJson(command2, Object.class);
		System.err.println(obj);
		System.err.println(obj.getClass());
		System.err.println("");	
	}
	
	public void initReader()
	{
		br = new BufferedReader(new InputStreamReader(System.in));
	}
	
	public void connect()
	{
		try {
			Socket clientSoc = new Socket("localhost", 9999);
			os = clientSoc.getOutputStream();
			is = clientSoc.getInputStream();
			connected = true;
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			connected = false;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			connected=false;
		}		
	}

	public void write() {
		try {



			String data = null;

			String json = "{\r\n" + "    \"glossary\": {\r\n" + "        \"title\": \"example glossary\",\r\n"
					+ "		\"GlossDiv\": {\r\n" + "            \"title\": \"S\",\r\n"
					+ "			\"GlossList\": {\r\n" + "                \"GlossEntry\": {\r\n"
					+ "                    \"ID\": \"SGML\",\r\n" + "					\"SortAs\": \"SGML\",\r\n"
					+ "					\"GlossTerm\": \"Standard Generalized Markup Language\",\r\n"
					+ "					\"Acronym\": \"SGML\",\r\n"
					+ "					\"Abbrev\": \"ISO 8879:1986\",\r\n" + "					\"GlossDef\": {\r\n"
					+ "                        \"para\": \"A meta-markup language, used to create markup languages such as DocBook.\",\r\n"
					+ "						\"GlossSeeAlso\": [\"GML\", \"XML\"]\r\n" + "                    },\r\n"
					+ "					\"GlossSee\": \"markup\"\r\n" + "                }\r\n" + "            }\r\n"
					+ "        }\r\n" + "    }\r\n" + "}";
			//json = "msg";
			/*
			for (int i = 0; i < 5; i++) {
				json = json + i;
				byte[] jsonbytes = pack(json);
				os.write(jsonbytes);
			}*/
			String prefix = Utility.getRandomString(5);
			System.err.println("Prefix being set.. " + prefix);
			String [] alldata = new String[] {"prefix", prefix};
			PayloadStruct pl = makePayload(alldata);			
			// pack and send
			byte[] finalByte = pack(gson.toJson(pl), pl.epoc);
			os.write(finalByte);
			
			String trialCommand = "{'insightId': 'TempInsight_fca2c035-6d85-4025-af8f-59656450b951', 'user': {'port': '9999', 'workspaceProjectMap': {}, 'assetProjectMap': {}, 'primaryLogin': 'NATIVE', 'accessTokens': {'Native': {'provider': 'NATIVE', 'userGroups': [], 'id': 'prabhuk12@gmail.com', 'expires_in': 0, 'token_type': 'Bearer', 'startTime': -1, 'email': 'prabhuk12@gmail.com', 'name': 'prabhuk', 'sans': {}, 'locked': False, 'lastLogin': {'strDate': '2023-07-02 12:09:15.385', 'pattern': 'yyyy-MM-dd HH:mm:ss.S'}}}, 'loggedInProfiles': ['NATIVE'], 'insightSecret': {}, 'sharedSessions': [], 'projectIdMap': {'eed12b32-bc38-4718-ab73-c0c78480c174': 'TAP_Site_Data', '31a4a3c7-2855-4fa7-8d55-3a96dfca9a7e': 'CPCE_Insights', '98d7ec5d-3e71-42c0-9fa5-f32e903c9134': 'Food', '92b0083f-0c22-4530-b2b8-4439a85d4e21': 'Mv_Audit', '3a7bf640-9129-4515-ab5e-6a54272743d7': 'Asset', '9f78e44f-64cc-456a-925f-b5062783315f': 'FrameTester', '0ee98941-3e27-472f-805e-50dbfc1b39a0': 'ASAALT', '75282231-f7b6-4f95-8be5-2099a32c9e94': 'EDAF Standards', '4c7c8c55-840f-47c8-b41a-a57e5b8dd84b': 'REngine', 'b7ef29ce-92b3-4720-aece-f626ac48c424': 'Diabetes2', '1e4c33f9-a7b7-48ed-aa89-e1546591f5bf': 'GitTest2', 'df7be6f8-9ec3-4707-9d68-bf91141281b9': 'DECOM', '820972d8-1df1-49f3-869d-9a12517e7ac7': 'Full PITM', '4ab3fdd3-898c-4115-9b96-e925efe7baa9': 'GitTest', '60d03e3c-5e50-46df-a7bb-12b2a51eac89': 'Diabetes Blood Glucose', '6981102e-215f-4613-94a3-91be40cf1de7': 'Patient Predict', 'cd170b5c-47c7-4c75-94c7-d8428cb4f695': 'Standards Pedigree', 'COUNT': '50', 'bc85f8ad-3dfc-48c3-8dd2-477021d2a61c': 'Navy Demo', '01d85d8d-5827-455e-898d-4ea5c30c71f8': 'Go Live Date', '7c7c8c55-840f-47c8-b41a-a57e5b8dd84b': 'Suppliers', 'fcee8e59-bb1d-4d0e-815d-0de8d265ddaf': 'IndependenceData', '049b3b6a-6630-4f77-8f3a-2892fec2188b': 'Blood Glucose', '6adbe3cc-ac05-454d-8d31-cb7df29c20b2': 'All Standards', '8879489f-0303-4e19-b12f-9fc0a0d54e82': 'D2D Market Info', '15f480f7-99c0-4b54-8ba6-e7080adb030d': 'Informatica', 'bd47b02f-49d8-469d-b0ec-de0c900ef652': 'Mango', '80e11662-2fe5-4597-9245-4b245e58a04d': 'Army_Demo', '0890ef56-d45b-4841-94f3-7d38e217b2fc': 'Attachment 12 - CHCS Data', '68959667-23d5-452c-b222-75687318a3f4': 'PITM PISM', 'cb8c6d2d-bce3-4477-ae99-5f2e08061ab2': 'Snowflake_PharmDB', 'f7ecd448-63d1-4bcb-a2a1-5c1dd85e5303': 'Temp', 'e54f4d69-be2c-456e-bd73-9ccd3d9e1e33': 'Shnowflake', '5270d6bf-52a2-4167-9301-23d6569eb782': 'Pharmacy Claims', '133db94b-4371-4763-bff9-edf7e5ed021b': 'TAP_Core_Data', 'bf47a518-1929-4ab7-9514-9ad524e0a6bc': 'Full PISM', 'aba4dcc6-1fe5-423e-8d08-1f534aa98c42': 'Pharmacy Malpractice', '6c7c8c55-840f-47c8-b41a-a57e5b8dd84b': 'Customers', '2b244110-5610-4641-b17c-cbb29181e2a3': 'NETCOM Tools', '43ae2362-cddd-4eff-83ed-d9bb2d726ca4': 'GitApp', 'ed700aed-1ba7-488f-824c-18be95367fbf': 'ATGR Linked', 'b210b43b-4272-43d6-9429-feacb4e7c9d9': 'BEA Database', '30991037-1e73-49f5-99d3-f28210e6b95c': 'DISR', 'b6fe5758-e69e-43a5-9918-0b61e3b4a6e0': 'Actor3', '854544eb-6488-4e24-b0ba-9f6f903be4f7': 'Fed_Head_Count', '80ffbf6e-ae31-4da3-92a3-0c17761e87d5': 'SDD System Contracts', 'b98c5d74-c410-4f83-93fa-939b4eccb8f0': 'GitApp2', '2179c054-262f-4e54-8aa3-4adb13be0911': 'APMS', 'aef11195-f757-4b01-8d12-724c85a65640': 'System Stage Tracker', '275d3315-039c-47e6-8c40-88bfda54c487': 'NDC Data Set', '5c7c8c55-840f-47c8-b41a-a57e5b8dd84b': 'Alliance'}, 'engineIdMap': {}, 'varMap': {}, 'anonymous': False, 'rPort': -1, 'pyPort': 9999, 'forcePort': -1, 'insightSerializedMap': {}}, 'cacheable': True, 'cacheMinutes': -1, 'cacheEncrypt': False, 'count': 0, 'isOldInsight': False, 'pragmap': {'xCache': 'false'}, 'baseURL': 'http://localhost:8080/appui/#!/', 'contextReinitialized': False, 'sqlWrapperMap': {}, 'id2SQLMapper': {}, 'idCount': 0}";
			pl = makePayload(trialCommand);			
			// pack and send
			finalByte = pack(gson.toJson(pl), pl.epoc);
			os.write(finalByte);
			
			finalByte = pack("forcing an error", "p123");
			os.write(finalByte);
			
			//shutdown();
			
			while (connected) {
				
				data = br.readLine(); 
				
				pl = makePayload(data);
				
				// pack and send
				finalByte = pack(gson.toJson(pl), pl.epoc);
				os.write(finalByte);
			}

			// os.write("hello there how are you doing ?".getBytes());
			os.write("close".getBytes());
		} catch (Exception ex) {
			System.err.println("Conn reset");
			// break;
		}
	}
	
	public PayloadStruct makePayload(String... data)
	{
		PayloadStruct pl = new PayloadStruct();
		pl.payload = data;
		pl.epoc = "e" + epocCounter++;
		pl.operation = PayloadStruct.OPERATION.PYTHON;
		pl.insightId = "f2749d4b-9468-418c-bb7a-cb6457367dbd";
		return pl;
	}

	public byte[] pack(String message, String epoc) {
		byte[] psBytes = message.getBytes(StandardCharsets.UTF_8);

		// get the length
		int length = psBytes.length;

		System.err.println("Packing with length " + length);

		// make this into array
		byte[] lenBytes = ByteBuffer.allocate(4).putInt(length).array();

		System.err.println(epoc.getBytes().length);
		
		byte[] epocBytes = ByteBuffer.allocate(20).put(epoc.getBytes(StandardCharsets.UTF_8)).array();

		// pack both of these
		byte[] finalByte = new byte[psBytes.length + lenBytes.length + epocBytes.length];

		for (int lenIndex = 0; lenIndex < lenBytes.length; lenIndex++)
			finalByte[lenIndex] = lenBytes[lenIndex];

		for (int lenIndex = 0; lenIndex < epocBytes.length; lenIndex++)
			finalByte[lenIndex + lenBytes.length] = epocBytes[lenIndex];

		for (int lenIndex = 0; lenIndex < psBytes.length; lenIndex++)
			finalByte[lenIndex + lenBytes.length + epocBytes.length] = psBytes[lenIndex];

		return finalByte;

	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		boolean stop = false;
		while (!stop) 
		{
			try {
				byte[] length = new byte[4];
				is.read(length);

				int size = ByteBuffer.wrap(length).getInt();
				System.err.println("Receiving Size.. " + size);

				if(size == 0)
				{
					stop = true;
					break;
				}
					
				//System.out.println("incoming size " + size);

				byte[] msg = new byte[size];
				int size_read = 0;
				while(size_read < size)
				{
					//System.out.println("Available bites.. " + is.available());
					int to_read = size - size_read;
					byte [] newMsg = new byte[to_read];
					int cur_size = is.read(newMsg);
					System.out.println("incoming size " + size + "  read size.. " + size_read + " cur zie " + cur_size);
					// for some reason the cur_size + size_read is bigger than that of size
					//if(cur_size + size_read > size)
					//	cur_size = size - size_read;
					System.arraycopy(newMsg, 0, msg, size_read, cur_size);
					size_read = size_read + cur_size;
				}

				String message = new String(msg);
				System.err.println(message);
				PayloadStruct ps = gson.fromJson(message, PayloadStruct.class);
				
				if(ps.response || ps.operation == ps.operation.STDOUT)
				{
					////////////////////////////////////////////////////////////
					// checking for serializations
					////////////////////////////////////////////////////////////
					System.err.print(ps.response + " <<>> " + ps.payload[0] + "<<>>" + ps.epoc + "<<>>" + ps.ex);
					String payload = (String)ps.payload[0];
					try
					{
						//System.err.println("What do you want to attempt to convert this to ?");
						//String userType = br.readLine();
						if(payload.startsWith("["))
						{
							//list(Diabetes_Patient_Demographics_FRAME6169w.cache['data'].columns) = List<String>
							//Diabetes_Patient_Demographics_FRAME6169w.cache['data'].dtypes.tolist() = List<String>
	
							List l = gson.fromJson((String)ps.payload[0], ArrayList.class);
							System.err.println("");
							
						}
						if(payload.startsWith("{"))
						{
							//Diabetes_Patient_Demographics_FRAME6169w.cache['data'][['AGE','BP_1D','BP_1S','BP_2D','BP_2S','CHOL','DIABETES_UNIQUE_ROW_ID','DRUG','END_DATE','FRAME','GENDER','GLYHB','HDL','HEIGHT','HIP','ID','LOCATION','RATIO','SEMOSS_EXPORT_20221024_233136_UNIQUE_ROW_ID','STAB_GLU','START_DATE','TIME_PPN','WAIST','WEIGHT']].drop_duplicates().sort_values(['AGE'],ascending=[True]).iloc[0:2000].to_dict('split')
							System.err.println(ps.payload[0]);
							Map m = gson.fromJson((String)ps.payload[0], Map.class);
							System.err.println("");
						}
						if(payload.startsWith("whatever"))
						{
							//('Diabetes_Patient_Demographics_FRAME6169w' in vars() and len(Diabetes_Patient_Demographics_FRAME6169w.cache['data']) >= 0)
							Boolean b = gson.fromJson((String)ps.payload[0], Boolean.class);
							System.err.println("");
						}
						
					}catch(Exception ex)
					{
						ex.printStackTrace();
					}
				}
				/////////////////////////////////////////////////////////////////////
				//////// Sending response back.. just echo here
				////////////////////////////////////////////////////////////////////
				
				else if(!ps.response)
				{
					System.err.println("This is waiting for a response back from here");
					System.err.println("Object.. " + ps.objId);
					System.err.println("Method Name.. " + ps.methodName);
					System.err.println("args.. " + ps.payload);
					String pickleInput = ps.payload[0] + "";
					ps.payload = new String[] {"Got the message " + ps.objId};
					ps.response = true;
					
					byte [] finalByte = pack(gson.toJson(ps), ps.epoc);
					os.write(finalByte);
					System.err.println("Sent output back.. ");
					
					
					// make the call to get pickle response as well
					if(ps.engineType.equalsIgnoreCase("pickler"))
					{
						ps.response = false;
						ps.operation = ps.operation.PYTHON;
						ps.payload = new String [] {"d.get_str_back('" + pickleInput + "')"};
						finalByte = pack(gson.toJson(ps), ps.epoc);
						os.write(finalByte);
					}					
				}
				
				
				//System.err.println(ps.operation);
			} catch (Exception ex) {
				ex.printStackTrace();
				stop = true;
				connected=false;
				break;
			}
		}
		connected = false;
		System.err.println("outside the run loop");
	}

	private void shutdown()
	{
    	try {
			PayloadStruct ps = new PayloadStruct();
			ps.methodName = "CLOSE_ALL_LOGOUT<o>";
			ps.payload = new String[] { "CLOSE_ALL_LOGOUT<o>"};
			ps.epoc="Shutting_down";
			byte [] finalByte = pack(gson.toJson(ps), ps.epoc);
			os.write(finalByte);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
