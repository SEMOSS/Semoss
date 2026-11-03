package prerna.ds.py;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.SystemUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import com.google.gson.Gson;

import prerna.algorithm.api.SemossDataType;
import prerna.om.Insight;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.reactor.AbstractReactor;
import prerna.reactor.frame.py.ImputeNullValuesReactor;
import prerna.reactor.frame.py.ReplaceColumnValueReactor;
import prerna.reactor.frame.py.UpdateRowValuesReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Runs the pandas code that PandasFrame, PandasSyntaxHelper and the py frame
 * reactors generate against the pandas actually installed in the platform venv.
 * The other pandas tests mock Python and only compare strings, so they cannot
 * catch a pandas upgrade that removes or changes an API.
 *
 * Uses py/install_config/.venv (relative to the Semoss project dir) unless
 * -Dsemoss.test.python or SEMOSS_TEST_PYTHON points at another interpreter;
 * skipped when no interpreter is found. pandas FutureWarning, DeprecationWarning
 * and ChainedAssignmentError are raised as errors so the next upgrade fails here
 * instead of silently in production.
 */
@Timeout(value = 2, unit = TimeUnit.MINUTES)
class PandasCompatibilityTests {

	// Mirrors the python server: exec every statement, eval a trailing expression,
	// answer with json default=str. One JSON-encoded script per line in and out.
	private static final String DRIVER = """
			import ast, json, sys, warnings
			sys.path.insert(0, sys.argv[1])
			import pandas as pd
			import clean  # before the filters so third-party import-time warnings do not count
			warnings.simplefilter('error', FutureWarning)
			warnings.simplefilter('error', DeprecationWarning)
			warnings.simplefilter('error', pd.errors.ChainedAssignmentError)
			out, sys.stdout = sys.stdout, sys.stderr
			g = {'__name__': '__smss__'}
			for line in sys.stdin:
			    try:
			        tree = ast.parse(json.loads(line))
			        last = tree.body.pop() if tree.body and isinstance(tree.body[-1], ast.Expr) else None
			        exec(compile(tree, '<smss>', 'exec'), g)
			        value = eval(compile(ast.Expression(last.value), '<smss>', 'eval'), g) if last else None
			        res = {'ok': True, 'result': value}
			    except BaseException as e:
			        res = {'ok': False, 'error': type(e).__name__ + ': ' + str(e)}
			    out.write(json.dumps(res, default=str) + '\\n')
			    out.flush()
			""";

	private static final Gson GSON = new Gson();
	private static final String IMPORTS = PandasFrame.PANDAS_IMPORT_STRING + "\n" + PandasFrame.NUMPY_IMPORT_STRING;

	private static Process python;
	private static BufferedWriter toPython;
	private static BufferedReader fromPython;
	private static PyTranslator translator;

	@TempDir
	Path tempDir;

	@BeforeAll
	static void startPython() throws IOException {
		Path interpreter = findPython();
		Assumptions.assumeTrue(Files.isExecutable(interpreter),
				"No python at " + interpreter.toAbsolutePath() + "; set -Dsemoss.test.python to run these tests");
		python = new ProcessBuilder(interpreter.toString(), "-I", "-c", DRIVER, Paths.get("py").toAbsolutePath().toString())
				.redirectError(ProcessBuilder.Redirect.INHERIT).start();
		toPython = new BufferedWriter(new OutputStreamWriter(python.getOutputStream(), StandardCharsets.UTF_8));
		fromPython = new BufferedReader(new InputStreamReader(python.getInputStream(), StandardCharsets.UTF_8));
		translator = realPandasTranslator();
		System.out.println("PandasCompatibilityTests running against pandas " + py(IMPORTS + "\npd.__version__"));
	}

	@AfterAll
	static void stopPython() {
		if (python != null) {
			python.destroyForcibly();
		}
	}

	@Test
	void typedCsvImport_parsesDatesAndValues() throws IOException {
		Map<String, SemossDataType> types = new LinkedHashMap<>();
		types.put("name", SemossDataType.STRING);
		types.put("amount", SemossDataType.DOUBLE);
		types.put("qty", SemossDataType.INT);
		types.put("when", SemossDataType.DATE);
		String script = PandasSyntaxHelper.getCsvFileRead(PandasFrame.PANDAS_IMPORT_VAR, PandasFrame.NUMPY_IMPORT_VAR,
				writeFixtureCsv(), "df", ",", "\"", "\\\\", null, types, -1);

		py(IMPORTS + "\n" + script);

		assertPy("df['name'].tolist() == ['Alice', 'Bob']");
		assertPy("df['qty'].tolist() == [3, 4]");
		assertPy("df['amount'].isna().tolist() == [False, True]");
		assertPy("str(df['when'].dtype).startswith('datetime64')");
	}

	@Test
	void convertingCsvImport_keepsLeadingZerosAndParsesNumbers() throws IOException {
		String script = PandasSyntaxHelper.getCsvFileRead(PandasFrame.PANDAS_IMPORT_VAR, PandasFrame.NUMPY_IMPORT_VAR,
				writeFixtureCsv(), "df", ",", "\"", "\\\\", "utf-8");

		py(IMPORTS + "\n" + script);

		assertPy("df['zip'].tolist() == ['02134', '10001']");
		assertPy("df['qty'].tolist() == [3, 4]");
		assertPy("df['amount'].isna().tolist() == [False, True]");
		assertPy("df['name'].tolist() == ['Alice', 'Bob']");
		// converters bypass read_csv's own NA detection, so these rely on NAN_VALUES
		assertPy("df['note'].isna().tolist() == [True, True]");
	}

	@Test
	void adjustDataTypes_convertsDatesAndLeavesUnparseableColumnsAlone() throws Exception {
		py(IMPORTS + "\n"
				+ "df = pd.DataFrame({'d': ['2024-01-02', '2024-02-03'], 't': ['2024-01-02 10:30:00', '2024-02-03 11:00:00'],"
				+ " 'bad': ['not a date', 'x'], 'n': ['1', '2']})\n" + PandasSyntaxHelper.makeWrapper("dfw", "df"));
		Map<String, SemossDataType> types = Map.of("d", SemossDataType.DATE, "t", SemossDataType.TIMESTAMP, "bad",
				SemossDataType.DATE, "n", SemossDataType.INT);
		Method adjust = PandasFrame.class.getDeclaredMethod("adjustDataTypes", String.class, Map.class);
		adjust.setAccessible(true);

		adjust.invoke(new PandasFrame("df", translator), "df", types);

		assertPy("type(df['d'].iloc[0]).__name__ == 'date'");
		assertPy("str(df['t'].dtype).startswith('datetime64')");
		assertPy("df['bad'].tolist() == ['not a date', 'x']");
		assertPy("str(df['n'].dtype) == 'int64'");
	}

	@Test
	void getHeaderAndTypes_mapsEveryRealDtype() {
		py(IMPORTS + "\ndf = pd.DataFrame({'s': ['a', 'b'], 'i': [1, 2], 'f': [1.5, 2.5], 'b': [True, False],"
				+ " 't': pd.to_datetime(['2024-01-02 10:30', '2024-01-03 11:45'])})");

		Object[] headerAndTypes = new PandasFrame("df", translator).getHeaderAndTypes("df");

		assertArrayEquals(new SemossDataType[] { SemossDataType.STRING, SemossDataType.INT, SemossDataType.DOUBLE,
				SemossDataType.BOOLEAN, SemossDataType.TIMESTAMP }, (SemossDataType[]) headerAndTypes[0]);
	}

	@Test
	void convertDataType_mapsRealColumnDtypeNames() {
		py(IMPORTS + "\ndf = pd.DataFrame({'s': ['a'], 'i': [1], 'f': [1.5], 'b': [True],"
				+ " 't': pd.to_datetime(['2024-01-02 10:30'])})");
		String[] columns = { "s", "i", "f", "b", "t" };
		SemossDataType[] expected = { SemossDataType.STRING, SemossDataType.INT, SemossDataType.DOUBLE,
				SemossDataType.BOOLEAN, SemossDataType.TIMESTAMP };
		PyTranslator realTranslator = new PyTranslator(null, null);

		for (int i = 0; i < columns.length; i++) {
			// same script AbstractPyFrameReactor.getColumnType runs
			String dtype = (String) py(PandasSyntaxHelper.getColumnType("df", columns[i]));
			assertEquals(expected[i], realTranslator.convertDataType(dtype), "pandas dtype " + dtype);
		}
	}

	@Test
	void replaceColumnValue_fillsNumericAndTimestampNulls() {
		py(IMPORTS + "\ndf = pd.DataFrame({'n': [1.0, None], 't': pd.to_datetime(['2024-01-02', None])})\n"
				+ PandasSyntaxHelper.makeWrapper("dfw", "df"));
		PandasFrame frame = new PandasFrame("df", translator);

		runReactor(new ReplaceColumnValueReactor(), frame, Map.of(ReactorKeysEnum.COLUMN.getKey(), List.of("n"),
				ReactorKeysEnum.VALUE.getKey(), List.of("null"), ReactorKeysEnum.NEW_VALUE.getKey(), List.of("0")));
		runReactor(new ReplaceColumnValueReactor(), frame,
				Map.of(ReactorKeysEnum.COLUMN.getKey(), List.of("t"), ReactorKeysEnum.VALUE.getKey(), List.of("null"),
						ReactorKeysEnum.NEW_VALUE.getKey(), List.of("2024-03-04")));

		assertPy("df['n'].tolist() == [1.0, 0.0]");
		assertPy("dfw.cache['data']['n'].tolist() == [1.0, 0.0]");
		assertPy("df['t'].iloc[1] == pd.Timestamp('2024-03-04')");
	}

	@Test
	void replaceColumnValue_replacesStringAndNumericValues() {
		py(IMPORTS + "\ndf = pd.DataFrame({'s': ['a', 'b', 'a'], 'n': [1.0, 2.0, 1.0]})\n"
				+ PandasSyntaxHelper.makeWrapper("dfw", "df"));
		PandasFrame frame = new PandasFrame("df", translator);

		runReactor(new ReplaceColumnValueReactor(), frame, Map.of(ReactorKeysEnum.COLUMN.getKey(), List.of("s"),
				ReactorKeysEnum.VALUE.getKey(), List.of("a"), ReactorKeysEnum.NEW_VALUE.getKey(), List.of("z\"q")));
		runReactor(new ReplaceColumnValueReactor(), frame, Map.of(ReactorKeysEnum.COLUMN.getKey(), List.of("n"),
				ReactorKeysEnum.VALUE.getKey(), List.of("1"), ReactorKeysEnum.NEW_VALUE.getKey(), List.of("5")));

		assertPy("df['s'].tolist() == ['z\"q', 'b', 'z\"q']");
		assertPy("df['n'].tolist() == [5.0, 2.0, 5.0]");
	}

	@Test
	void replaceColumnValue_fillsStringNullsWithQuotesAndBackslashes() {
		py(IMPORTS + "\ndf = pd.DataFrame({'s': ['a', None]})\n" + PandasSyntaxHelper.makeWrapper("dfw", "df"));

		runReactor(new ReplaceColumnValueReactor(), new PandasFrame("df", translator),
				Map.of(ReactorKeysEnum.COLUMN.getKey(), List.of("s"), ReactorKeysEnum.VALUE.getKey(), List.of("null"),
						ReactorKeysEnum.NEW_VALUE.getKey(), List.of("say \"hi\" \\ ok")));

		assertPy("df['s'].tolist() == ['a', 'say \"hi\" \\\\ ok']");
	}

	@Test
	void valueReactors_rejectUnsupportedColumnTypesWithClearErrors() {
		py(IMPORTS + "\ndf = pd.DataFrame({'b': [True, False], 'd': pd.to_timedelta(['1 day', '2 days'])})\n"
				+ PandasSyntaxHelper.makeWrapper("dfw", "df"));
		PandasFrame frame = new PandasFrame("df", translator);

		IllegalArgumentException bool = assertThrows(IllegalArgumentException.class,
				() -> runReactor(new ReplaceColumnValueReactor(), frame,
						Map.of(ReactorKeysEnum.COLUMN.getKey(), List.of("b"), ReactorKeysEnum.VALUE.getKey(),
								List.of("true"), ReactorKeysEnum.NEW_VALUE.getKey(), List.of("false"))));
		IllegalArgumentException unmapped = assertThrows(IllegalArgumentException.class,
				() -> runReactor(new ReplaceColumnValueReactor(), frame,
						Map.of(ReactorKeysEnum.COLUMN.getKey(), List.of("d"), ReactorKeysEnum.VALUE.getKey(),
								List.of("x"), ReactorKeysEnum.NEW_VALUE.getKey(), List.of("y"))));

		assertTrue(bool.getMessage().contains("BOOLEAN"), bool.getMessage());
		assertTrue(unmapped.getMessage().contains("timedelta64"), unmapped.getMessage());
		assertPy("df['b'].tolist() == [True, False]");
	}

	@Test
	void updateRowValues_setsNumericNullTimestampAndStringValues() {
		py(IMPORTS + "\ndf = pd.DataFrame({'id': [1, 2], 'n': [1.0, 2.0],"
				+ " 't': pd.to_datetime(['2024-01-02 10:00:00', '2024-01-03 10:00:00']), 's': ['a', 'b']})\n"
				+ PandasSyntaxHelper.makeWrapper("dfw", "df"));
		PandasFrame frame = new PandasFrame("df", translator);
		// builds the frame metadata from the real pandas dtypes, as the platform does after a python edit
		frame.recreateMeta();
		Map<String, SemossDataType> metaTypes = frame.getMetaData().getHeaderToTypeMap();
		assertEquals(SemossDataType.INT, metaTypes.get("df__id"));
		assertEquals(SemossDataType.DOUBLE, metaTypes.get("df__n"));
		assertEquals(SemossDataType.TIMESTAMP, metaTypes.get("df__t"));
		assertEquals(SemossDataType.STRING, metaTypes.get("df__s"));

		String[][] updates = { { "n", "null" }, { "t", "2024-05-06 07:08:09" }, { "s", "say \"hi\"" } };
		for (String[] update : updates) {
			SelectQueryStruct qs = new SelectQueryStruct();
			qs.addExplicitFilter(new SimpleQueryFilter(
					new NounMetadata(new QueryColumnSelector("df__id"), PixelDataType.COLUMN), "==",
					new NounMetadata(1, PixelDataType.CONST_INT)));
			AbstractReactor reactor = prepareReactor(new UpdateRowValuesReactor(), frame, Map
					.of(ReactorKeysEnum.COLUMN.getKey(), List.of(update[0]), ReactorKeysEnum.VALUE.getKey(), List.of(update[1])));
			reactor.getNounStore().makeGenRowStruct(ReactorKeysEnum.QUERY_STRUCT.getKey())
					.add(new NounMetadata(qs, PixelDataType.QUERY_STRUCT));
			reactor.execute();
		}

		assertPy("df['n'].isna().tolist() == [True, False]");
		assertPy("df['t'].tolist() == [pd.Timestamp('2024-05-06 07:08:09'), pd.Timestamp('2024-01-03 10:00:00')]");
		assertPy("df['s'].tolist() == ['say \"hi\"', 'b']");
	}

	@Test
	void imputeNullValues_fillsNumericTimestampAndStringNulls() {
		py(IMPORTS + "\ndf = pd.DataFrame({'n': [1.0, None, 3.0], 't': pd.to_datetime(['2024-01-02', None, '2024-01-04']),"
				+ " 's': ['a', 'a', None]})\n" + PandasSyntaxHelper.makeWrapper("dfw", "df"));

		runReactor(new ImputeNullValuesReactor(), new PandasFrame("df", translator),
				Map.of(ReactorKeysEnum.COLUMNS.getKey(), List.of("n", "t", "s")));

		assertPy("df['n'].tolist() == [1.0, 2.0, 3.0]");
		assertPy("df['t'].iloc[1] == pd.Timestamp('2024-01-02')");
		assertPy("df['s'].tolist() == ['a', 'a', 'a']");
	}

	private String writeFixtureCsv() throws IOException {
		Path csv = tempDir.resolve("fixture.csv");
		Files.writeString(csv,
				"name,amount,qty,when,zip,note\nAlice,1.5,3,2024-01-02,02134,NULL\nBob,,4,2024-02-03,10001,n/a\n");
		return csv.toAbsolutePath().toString();
	}

	private static void runReactor(AbstractReactor reactor, PandasFrame frame, Map<String, List<String>> keys) {
		prepareReactor(reactor, frame, keys).execute();
	}

	private static AbstractReactor prepareReactor(AbstractReactor reactor, PandasFrame frame,
			Map<String, List<String>> keys) {
		Insight insight = mock(Insight.class);
		when(insight.getDataMaker()).thenReturn(frame);
		when(insight.getPyTranslator()).thenReturn(translator);
		reactor.In();
		reactor.setInsight(insight);
		keys.forEach((key, values) -> {
			GenRowStruct grs = reactor.getNounStore().makeGenRowStruct(key);
			values.forEach(value -> grs.add(new NounMetadata(value, PixelDataType.CONST_STRING)));
		});
		return reactor;
	}

	private static void assertPy(String expression) {
		assertEquals(Boolean.TRUE, py(expression), expression);
	}

	/**
	 * A PyTranslator whose script-running methods execute in the real python
	 * process; convertDataType keeps its real Java mapping.
	 */
	private static PyTranslator realPandasTranslator() {
		return mock(PyTranslator.class, invocation -> {
			Method method = invocation.getMethod();
			if (method.getDeclaringClass() == Object.class || method.getName().equals("convertDataType")) {
				return invocation.callRealMethod();
			}
			StringBuilder script = new StringBuilder();
			for (Object arg : invocation.getArguments()) {
				if (arg instanceof String s) {
					script.append(s).append('\n');
				} else if (arg instanceof String[] lines) {
					for (String line : lines) {
						if (line != null) {
							script.append(line).append('\n');
						}
					}
				}
			}
			String code = method.getName().equals("getColumns") ? "list(" + script.toString().trim() + ".columns)"
					: script.toString();
			Object result = py(code);
			Class<?> returnType = method.getReturnType();
			if (returnType == String[].class) {
				return ((List<?>) result).toArray(new String[0]);
			} else if (returnType == Long.class || returnType == long.class) {
				return ((Number) result).longValue();
			} else if (returnType == int.class) {
				return ((Number) result).intValue();
			} else if (returnType == double.class) {
				return ((Number) result).doubleValue();
			}
			return result;
		});
	}

	private static Object py(String code) {
		try {
			toPython.write(GSON.toJson(code));
			toPython.newLine();
			toPython.flush();
			String line = fromPython.readLine();
			if (line == null) {
				throw new AssertionError("python process exited; see stderr above");
			}
			Map<?, ?> response = GSON.fromJson(line, Map.class);
			if (!Boolean.TRUE.equals(response.get("ok"))) {
				throw new AssertionError("python failed: " + response.get("error") + "\n--- script ---\n" + code);
			}
			return response.get("result");
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	private static Path findPython() {
		String configured = System.getProperty("semoss.test.python", System.getenv("SEMOSS_TEST_PYTHON"));
		if (configured != null && !configured.isBlank()) {
			return Paths.get(configured);
		}
		Path venv = Paths.get("py", "install_config", ".venv");
		return SystemUtils.IS_OS_WINDOWS ? venv.resolve("Scripts").resolve("python.exe") : venv.resolve("bin").resolve("python");
	}
}
