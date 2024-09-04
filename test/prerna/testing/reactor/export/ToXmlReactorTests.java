package prerna.testing.reactor.export;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.algorithm.api.SemossDataType;
import prerna.reactor.export.IterateReactor;
import prerna.reactor.export.ToXmlReactor;
import prerna.reactor.imports.ImportReactor;
import prerna.reactor.imports.MergeReactor;
import prerna.reactor.qs.JoinReactor;
import prerna.reactor.qs.QueryAllReactor;
import prerna.reactor.qs.source.DatabaseReactor;
import prerna.reactor.qs.source.FrameReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.testing.AbstractBaseSemossApiTests;
import prerna.testing.ApiSemossTestEngineUtils;
import prerna.testing.ApiSemossTestInsightUtils;
import prerna.testing.ApiSemossTestUtils;
import prerna.testing.PixelChain;

public class ToXmlReactorTests extends AbstractBaseSemossApiTests {

	@Test
	public void testToXML() throws IOException {
		// Create Engine
		List<String> columns = new ArrayList<>();
		columns.add("colone");
		columns.add("coltwo");

		List<String> dtypes = new ArrayList<>();
		dtypes.add(SemossDataType.STRING.toString());
		dtypes.add(SemossDataType.INT.toString());
		Map<String, String> adt = new HashMap<>();

		List<List<String>> vals = new ArrayList<>();
		List<String> v1 = new ArrayList<>();
		List<String> v2 = new ArrayList<>();
		vals.add(v1);
		vals.add(v2);

		v1.add("jeff");
		v1.add("1");

		v2.add("jeff2");
		v2.add("2");

		String engine = ApiSemossTestEngineUtils.addTestRdbmsDatabase("test", columns, dtypes, adt, vals);

		// run toxml reactor
		PixelChain db = new PixelChain(DatabaseReactor.class, ReactorKeysEnum.DATABASE.getKey(), engine);
		PixelChain select = new PixelChain("Select(TEST__colone, TEST__coltwo).as([colone, coltwo])");
		PixelChain iterate = new PixelChain(IterateReactor.class);
		PixelChain toxml = new PixelChain(ToXmlReactor.class, ReactorKeysEnum.FILE_NAME.getKey(), "output");

		String pixel = ApiSemossTestUtils.buildPixelChain(db, select, iterate, toxml);
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		assertNotNull(nm.getValue());

		// read file
		Path pathToFile = Files.list(ApiSemossTestInsightUtils.getInsightCache())
				.filter(s -> s.toString().contains("output")).findFirst()
				.orElseThrow(() -> new RuntimeException("Could not find file"));

		List<String> linesFromXml = Files.readAllLines(pathToFile);
		assertEquals(4, linesFromXml.size());

		List<String> lines = new ArrayList<>();
		lines.add("<DataTable>");
		lines.add("<TEST><colone>\"jeff\"</colone><coltwo>1</coltwo></TEST>");
		lines.add("<TEST><colone>\"jeff2\"</colone><coltwo>2</coltwo></TEST>");
		lines.add("</DataTable>");

		for (int i = 0; i < lines.size(); i++) {
			assertEquals(lines.get(i), linesFromXml.get(i));
		}
	}

	@Test
	public void testOneRow() throws IOException {
		// Create Engine
		List<String> columns = new ArrayList<>();
		columns.add("colone");
		columns.add("coltwo");

		List<String> dtypes = new ArrayList<>();
		dtypes.add(SemossDataType.STRING.toString());
		dtypes.add(SemossDataType.INT.toString());
		Map<String, String> adt = new HashMap<>();

		List<List<String>> vals = new ArrayList<>();
		List<String> v1 = new ArrayList<>();
		vals.add(v1);

		v1.add("jeff");
		v1.add("1");

		String engine = ApiSemossTestEngineUtils.addTestRdbmsDatabase("test", columns, dtypes, adt, vals);

		// run toxml reactor
		PixelChain db = new PixelChain(DatabaseReactor.class, ReactorKeysEnum.DATABASE.getKey(), engine);
		PixelChain select = new PixelChain("Select(TEST__colone, TEST__coltwo).as([colone, coltwo])");
		PixelChain iterate = new PixelChain(IterateReactor.class);
		PixelChain toxml = new PixelChain(ToXmlReactor.class, ReactorKeysEnum.FILE_NAME.getKey(), "output");

		String pixel = ApiSemossTestUtils.buildPixelChain(db, select, iterate, toxml);
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		assertNotNull(nm.getValue());

		// read file
		Path pathToFile = Files.list(ApiSemossTestInsightUtils.getInsightCache())
				.filter(s -> s.toString().contains("output")).findFirst()
				.orElseThrow(() -> new RuntimeException("Could not find file"));

		List<String> linesFromXml = Files.readAllLines(pathToFile);
		assertEquals(3, linesFromXml.size());

		List<String> lines = new ArrayList<>();
		lines.add("<DataTable>");
		lines.add("<TEST><colone>\"jeff\"</colone><coltwo>1</coltwo></TEST>");
		lines.add("</DataTable>");

		for (int i = 0; i < lines.size(); i++) {
			assertEquals(lines.get(i), linesFromXml.get(i));
		}
	}

	@Test
	public void testBoolean() throws IOException {
		// Create Engine
		List<String> columns = new ArrayList<>();
		columns.add("cone");

		List<String> dtypes = new ArrayList<>();
		dtypes.add(SemossDataType.BOOLEAN.toString());

		Map<String, String> adt = new HashMap<>();

		List<List<String>> vals = new ArrayList<>();
		List<String> v1 = new ArrayList<>();
		vals.add(v1);

		v1.add("true");

		String engine = ApiSemossTestEngineUtils.addTestRdbmsDatabase("test", columns, dtypes, adt, vals);

		// run toxml reactor
		PixelChain db = new PixelChain(DatabaseReactor.class, ReactorKeysEnum.DATABASE.getKey(), engine);
		PixelChain select = new PixelChain("Select(TEST__cone).as([cone])");
		PixelChain iterate = new PixelChain(IterateReactor.class);
		PixelChain toxml = new PixelChain(ToXmlReactor.class, ReactorKeysEnum.FILE_NAME.getKey(), "output");

		String pixel = ApiSemossTestUtils.buildPixelChain(db, select, iterate, toxml);
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		assertNotNull(nm.getValue());

		// read file
		Path pathToFile = Files.list(ApiSemossTestInsightUtils.getInsightCache())
				.filter(s -> s.toString().contains("output")).findFirst()
				.orElseThrow(() -> new RuntimeException("Could not find file"));

		List<String> linesFromXml = Files.readAllLines(pathToFile);
		assertEquals(3, linesFromXml.size());

		List<String> lines = new ArrayList<>();
		lines.add("<DataTable>");
		lines.add("<TEST><cone>true</cone></TEST>");
		lines.add("</DataTable>");

		for (int i = 0; i < lines.size(); i++) {
			assertEquals(lines.get(i), linesFromXml.get(i));
		}
	}

	@Test
	public void testDouble() throws IOException {
		// Create Engine
		List<String> columns = new ArrayList<>();
		columns.add("cone");

		List<String> dtypes = new ArrayList<>();
		dtypes.add(SemossDataType.DOUBLE.toString());

		Map<String, String> adt = new HashMap<>();

		List<List<String>> vals = new ArrayList<>();
		List<String> v1 = new ArrayList<>();
		vals.add(v1);

		v1.add("1.1");

		String engine = ApiSemossTestEngineUtils.addTestRdbmsDatabase("test", columns, dtypes, adt, vals);

		// run toxml reactor
		PixelChain db = new PixelChain(DatabaseReactor.class, ReactorKeysEnum.DATABASE.getKey(), engine);
		PixelChain select = new PixelChain("Select(TEST__cone).as([cone])");
		PixelChain iterate = new PixelChain(IterateReactor.class);
		PixelChain toxml = new PixelChain(ToXmlReactor.class, ReactorKeysEnum.FILE_NAME.getKey(), "output");

		String pixel = ApiSemossTestUtils.buildPixelChain(db, select, iterate, toxml);
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		assertNotNull(nm.getValue());

		// read file
		Path pathToFile = Files.list(ApiSemossTestInsightUtils.getInsightCache())
				.filter(s -> s.toString().contains("output")).findFirst()
				.orElseThrow(() -> new RuntimeException("Could not find file"));

		List<String> linesFromXml = Files.readAllLines(pathToFile);
		assertEquals(3, linesFromXml.size());

		List<String> lines = new ArrayList<>();
		lines.add("<DataTable>");
		lines.add("<TEST><cone>1.1</cone></TEST>");
		lines.add("</DataTable>");

		for (int i = 0; i < lines.size(); i++) {
			assertEquals(lines.get(i), linesFromXml.get(i));
		}
	}

	@Test
	public void testDate() throws IOException {
		// Create Engine
		List<String> columns = new ArrayList<>();
		columns.add("cone");

		List<String> dtypes = new ArrayList<>();
		dtypes.add(SemossDataType.DATE.toString());

		Map<String, String> adt = new HashMap<>();

		List<List<String>> vals = new ArrayList<>();
		List<String> v1 = new ArrayList<>();
		vals.add(v1);

		v1.add("2024-01-01");

		String engine = ApiSemossTestEngineUtils.addTestRdbmsDatabase("test", columns, dtypes, adt, vals);

		// run toxml reactor
		PixelChain db = new PixelChain(DatabaseReactor.class, ReactorKeysEnum.DATABASE.getKey(), engine);
		PixelChain select = new PixelChain("Select(TEST__cone).as([cone])");
		PixelChain iterate = new PixelChain(IterateReactor.class);
		PixelChain toxml = new PixelChain(ToXmlReactor.class, ReactorKeysEnum.FILE_NAME.getKey(), "output");

		String pixel = ApiSemossTestUtils.buildPixelChain(db, select, iterate, toxml);
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		assertNotNull(nm.getValue());

		// read file
		Path pathToFile = Files.list(ApiSemossTestInsightUtils.getInsightCache())
				.filter(s -> s.toString().contains("output")).findFirst()
				.orElseThrow(() -> new RuntimeException("Could not find file"));

		List<String> linesFromXml = Files.readAllLines(pathToFile);
		assertEquals(3, linesFromXml.size());

		List<String> lines = new ArrayList<>();
		lines.add("<DataTable>");
		lines.add("<TEST><cone>2024-01-01</cone></TEST>");
		lines.add("</DataTable>");

		for (int i = 0; i < lines.size(); i++) {
			assertEquals(lines.get(i), linesFromXml.get(i));
		}
	}

	@Test
	public void testTimestamp() throws IOException {
		// Create Engine
		List<String> columns = new ArrayList<>();
		columns.add("cone");

		List<String> dtypes = new ArrayList<>();
		dtypes.add(SemossDataType.TIMESTAMP.toString());

		Map<String, String> adt = new HashMap<>();

		List<List<String>> vals = new ArrayList<>();
		List<String> v1 = new ArrayList<>();
		vals.add(v1);

		LocalDateTime ld = LocalDateTime.of(2024, 1, 1, 1, 1, 1);
		v1.add(ld.toString());

		adt.put("cone", "uuuu-MM-dd'T'HH:mm:ss");

		String engine = ApiSemossTestEngineUtils.addTestRdbmsDatabase("test", columns, dtypes, adt, vals);

		// run toxml reactor
		PixelChain db = new PixelChain(DatabaseReactor.class, ReactorKeysEnum.DATABASE.getKey(), engine);
		PixelChain select = new PixelChain("Select(TEST__cone).as([cone])");
		PixelChain iterate = new PixelChain(IterateReactor.class);
		PixelChain toxml = new PixelChain(ToXmlReactor.class, ReactorKeysEnum.FILE_NAME.getKey(), "output");

		String pixel = ApiSemossTestUtils.buildPixelChain(db, select, iterate, toxml);
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		assertNotNull(nm.getValue());

		// read file
		Path pathToFile = Files.list(ApiSemossTestInsightUtils.getInsightCache())
				.filter(s -> s.toString().contains("output")).findFirst()
				.orElseThrow(() -> new RuntimeException("Could not find file"));

		List<String> linesFromXml = Files.readAllLines(pathToFile);
		assertEquals(3, linesFromXml.size());

		List<String> lines = new ArrayList<>();
		lines.add("<DataTable>");
		lines.add("<TEST><cone>2024-01-01 00:01:01</cone></TEST>");
		lines.add("</DataTable>");

		for (int i = 0; i < lines.size(); i++) {
			assertEquals(lines.get(i), linesFromXml.get(i));
		}
	}

	@Test
	public void testToXMLNoTableName() throws IOException {
		// Create Engine
		List<String> columns = new ArrayList<>();
		columns.add("colone");
		columns.add("coltwo");

		List<String> dtypes = new ArrayList<>();
		dtypes.add(SemossDataType.STRING.toString());
		dtypes.add(SemossDataType.INT.toString());
		Map<String, String> adt = new HashMap<>();

		List<List<String>> vals = new ArrayList<>();
		List<String> v1 = new ArrayList<>();
		List<String> v2 = new ArrayList<>();
		vals.add(v1);
		vals.add(v2);

		v1.add("jeff");
		v1.add("1");

		v2.add("jeff2");
		v2.add("2");

		String engine = ApiSemossTestEngineUtils.addTestRdbmsDatabase("test", columns, dtypes, adt, vals);
		// run toxml reactor -> Database() | Select().as() | Iterate() | ToXml()
		// change to -> Frame() | QueryAll() | ToXml()
		PixelChain db = new PixelChain(DatabaseReactor.class, ReactorKeysEnum.DATABASE.getKey(), engine);
		PixelChain select = new PixelChain("Select(TEST__colone, TEST__coltwo).as([colone, coltwo])");
		PixelChain frame = new PixelChain(FrameReactor.class);
		PixelChain iterate = new PixelChain(IterateReactor.class);
		PixelChain toxml = new PixelChain(ToXmlReactor.class, ReactorKeysEnum.FILE_NAME.getKey(), "output");
		PixelChain queryAll = new PixelChain(QueryAllReactor.class);
		PixelChain imp = new PixelChain(ImportReactor.class);
		String pixel = ApiSemossTestUtils.buildPixelChain(db, select, imp, frame, queryAll, iterate, toxml);
		System.out.println(pixel); // Database(database="c07675e1-d489-4297-b3ea-2c6ccbfc9ff2") |
									// Select(TEST__colone, TEST__coltwo).as([colone, coltwo]) | Frame() | Iterate()
									// | ToXml(fileName="output");

		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		assertNotNull(nm.getValue());

		// read file
		Path pathToFile = Files.list(ApiSemossTestInsightUtils.getInsightCache())
				.filter(s -> s.toString().contains("output")).findFirst()
				.orElseThrow(() -> new RuntimeException("Could not find file"));

		List<String> linesFromXml = Files.readAllLines(pathToFile);
		assertEquals(4, linesFromXml.size());

		List<String> lines = new ArrayList<>();
		lines.add("<DataTable>");
		lines.add("<colone>\"jeff\"</colone><coltwo>1</coltwo>");
		lines.add("<colone>\"jeff2\"</colone><coltwo>2</coltwo>");
		lines.add("</DataTable>");

		for (int i = 0; i < lines.size(); i++) {
			assertEquals(lines.get(i), linesFromXml.get(i));
		}
	}

	@Test
	public void testNull() throws IOException {
		// Create Engine
		List<String> columns = new ArrayList<>();
		columns.add("cone");

		List<String> dtypes = new ArrayList<>();
		dtypes.add(SemossDataType.STRING.toString());

		Map<String, String> adt = new HashMap<>();

		List<List<String>> vals = new ArrayList<>();
		List<String> v1 = new ArrayList<>();
		vals.add(v1);

		v1.add(null);

		String engine = ApiSemossTestEngineUtils.addTestRdbmsDatabase("test", columns, dtypes, adt, vals);

		// run toxml reactor
		PixelChain db = new PixelChain(DatabaseReactor.class, ReactorKeysEnum.DATABASE.getKey(), engine);
		PixelChain select = new PixelChain("Select(TEST__cone).as([cone])");
		PixelChain iterate = new PixelChain(IterateReactor.class);
		PixelChain toxml = new PixelChain(ToXmlReactor.class, ReactorKeysEnum.FILE_NAME.getKey(), "output");

		String pixel = ApiSemossTestUtils.buildPixelChain(db, select, iterate, toxml);
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		assertNotNull(nm.getValue());

		// read file
		Path pathToFile = Files.list(ApiSemossTestInsightUtils.getInsightCache())
				.filter(s -> s.toString().contains("output")).findFirst()
				.orElseThrow(() -> new RuntimeException("Could not find file"));

		List<String> linesFromXml = Files.readAllLines(pathToFile);
		assertEquals(3, linesFromXml.size());

		List<String> lines = new ArrayList<>();
		lines.add("<DataTable>");
		lines.add("<TEST><cone>null</cone></TEST>");
		lines.add("</DataTable>");

		for (int i = 0; i < lines.size(); i++) {
			assertEquals(lines.get(i), linesFromXml.get(i));
		}
	}

	// join two tables using the join reactor and toXML
	@Test
	public void testDatabaseTableJoin() throws IOException {
		// Create Engine 1
		List<String> columns1 = new ArrayList<>();
		columns1.add("id");
		columns1.add("value1");

		List<String> dtypes1 = new ArrayList<>();
		dtypes1.add(SemossDataType.INT.toString());
		dtypes1.add(SemossDataType.STRING.toString());

		Map<String, String> adt1 = new HashMap<>();

		List<List<String>> vals1 = new ArrayList<>();
		List<String> v1_1 = new ArrayList<>();
		List<String> v2_1 = new ArrayList<>();
		vals1.add(v1_1);
		vals1.add(v2_1);

		v1_1.add("1");
		v1_1.add("value1_row1");

		v2_1.add("2");
		v2_1.add("value1_row2");

		String engine1 = ApiSemossTestEngineUtils.addTestRdbmsDatabase("test1", columns1, dtypes1, adt1, vals1);

		// Create Engine 2
		List<String> columns2 = new ArrayList<>();
		columns2.add("id");
		columns2.add("value2");

		List<String> dtypes2 = new ArrayList<>();
		dtypes2.add(SemossDataType.INT.toString());
		dtypes2.add(SemossDataType.STRING.toString());

		Map<String, String> adt2 = new HashMap<>();

		List<List<String>> vals2 = new ArrayList<>();
		List<String> v1_2 = new ArrayList<>();
		List<String> v2_2 = new ArrayList<>();
		vals2.add(v1_2);
		vals2.add(v2_2);

		v1_2.add("1");
		v1_2.add("value2_row1");

		v2_2.add("2");
		v2_2.add("value2_row2");

		String engine2 = ApiSemossTestEngineUtils.addTestRdbmsDatabase("test2", columns2, dtypes2, adt2, vals2);

		
		PixelChain db1 = new PixelChain(DatabaseReactor.class, ReactorKeysEnum.DATABASE.getKey(), engine1);
		PixelChain db2 = new PixelChain(DatabaseReactor.class, ReactorKeysEnum.DATABASE.getKey(), engine2);

		PixelChain select1 = new PixelChain("Select ( TEST1__id , TEST1__value1 ) .as ( [ id , value1 ] )");
		PixelChain select2 = new PixelChain("Select ( TEST2__id , TEST2__value2 ) .as ( [ id , value2 ] )");

		PixelChain notdistinct = new PixelChain("Distinct(false)");
		PixelChain distinct = new PixelChain("Distinct(true)");
		PixelChain import1 = new PixelChain("Import(frame=[CreateFrame(frameType=[GRID], override=[true]).as([\"test1frame\"])])");
		PixelChain import2 = new PixelChain("Import(frame=[CreateFrame(frameType=[GRID], override=[true]).as([\"test2frame\"])])");

		PixelChain queryall = new PixelChain(QueryAllReactor.class);
		PixelChain merge = new PixelChain("Merge ( joins = [ ( id , inner.join , id ) ] , frame = [ test2frame ] )");
		PixelChain toxml = new PixelChain(ToXmlReactor.class, ReactorKeysEnum.FILE_NAME.getKey(), "output");
		PixelChain holder = new PixelChain("Frame(test1frame)");

		// Build and process pixel chain
		String pixel1 = ApiSemossTestUtils.buildPixelChain(db1, select1, notdistinct, import1);
		ApiSemossTestUtils.processPixel(pixel1);

		String pixel2 = ApiSemossTestUtils.buildPixelChain(db2, select2, notdistinct, import2);
		ApiSemossTestUtils.processPixel(pixel2);

		String mergePixel = ApiSemossTestUtils.buildPixelChain(holder, queryall, distinct, merge, queryall, toxml);

		System.out.println(mergePixel);
		NounMetadata nm = ApiSemossTestUtils.processPixel(mergePixel);
		assertNotNull(nm.getValue());

		// Read file
		Path pathToFile = Files.list(ApiSemossTestInsightUtils.getInsightCache())
		        .filter(s -> s.toString().contains("output")).findFirst()
		        .orElseThrow(() -> new RuntimeException("Could not find file"));

		List<String> linesFromXml = Files.readAllLines(pathToFile);


		List<String> expectedLines = new ArrayList<>();
		expectedLines.add("<DataTable>");
		expectedLines.add("<id>1</id><value1>\"value1_row1\"</value1><value2>\"value2_row1\"</value2>");
		expectedLines.add("<id>2</id><value1>\"value1_row2\"</value1><value2>\"value2_row2\"</value2>");
		expectedLines.add("</DataTable>");

		for (int i = 0; i < expectedLines.size(); i++) {
		    assertEquals(expectedLines.get(i), linesFromXml.get(i));
		}
	}

	// join two Frames using the join reactor and toXML
	@Test
	public void testFrameJoin() throws IOException {
		// Create Engine
		List<String> columns = new ArrayList<>();
		columns.add("cone");

		List<String> dtypes = new ArrayList<>();
		dtypes.add(SemossDataType.STRING.toString());

		Map<String, String> adt = new HashMap<>();

		List<List<String>> vals = new ArrayList<>();
		List<String> v1 = new ArrayList<>();
		vals.add(v1);

		v1.add(null);

		String engine = ApiSemossTestEngineUtils.addTestRdbmsDatabase("test", columns, dtypes, adt, vals);

		// run toxml reactor
		PixelChain db = new PixelChain(DatabaseReactor.class, ReactorKeysEnum.DATABASE.getKey(), engine);
		PixelChain select = new PixelChain("Select(TEST__cone).as([cone])");
		PixelChain iterate = new PixelChain(IterateReactor.class);
		// import another db and try join select.join OR play around with it
		PixelChain toxml = new PixelChain(ToXmlReactor.class, ReactorKeysEnum.FILE_NAME.getKey(), "output");

		String pixel = ApiSemossTestUtils.buildPixelChain(db, select, iterate, toxml);
		NounMetadata nm = ApiSemossTestUtils.processPixel(pixel);
		assertNotNull(nm.getValue());

		// read file
		Path pathToFile = Files.list(ApiSemossTestInsightUtils.getInsightCache())
				.filter(s -> s.toString().contains("output")).findFirst()
				.orElseThrow(() -> new RuntimeException("Could not find file"));

		List<String> linesFromXml = Files.readAllLines(pathToFile);
		assertEquals(3, linesFromXml.size());

		List<String> lines = new ArrayList<>();
		lines.add("<DataTable>");
		lines.add("<TEST><cone>null</cone></TEST>");
		lines.add("</DataTable>");

		for (int i = 0; i < lines.size(); i++) {
			assertEquals(lines.get(i), linesFromXml.get(i));
		}
	}
}
