package prerna.testing.reactor.export;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.algorithm.api.SemossDataType;
import prerna.reactor.export.IterateReactor;
import prerna.reactor.export.ToXmlReactor;
import prerna.reactor.qs.source.DatabaseReactor;
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
	public void testToXMLOneDataRow() throws IOException {
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

}
