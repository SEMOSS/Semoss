package prerna.auth.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.javatuples.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.auth.access.ModelUsageLimit;
import prerna.auth.access.ModelUsageLimitKey;
import prerna.auth.access.ModelUsageLimitRule;
import prerna.auth.access.ModelUsageLimitRule.Counts;
import prerna.auth.access.ModelUsageLimitRule.Metric;
import prerna.auth.access.ModelUsageLimitRule.Window;
import prerna.engine.api.IRDBMSEngine;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;

/**
 * The copy of the four limit columns into MODELUSAGELIMIT, run again as a
 * second server starting at the same time would
 */
@SuppressWarnings("deprecation")
class ModelUsageLimitColumnMigrationUnitTests extends AbstractSecurityUtilsUnitTestsSetup {

	private static final String[][] ENGINE_LIMIT_COLUMNS = { { "USAGERESTRICTION", "VARCHAR(255)" },
			{ "USAGEFREQUENCY", "VARCHAR(255)" }, { "MAXTOKENS", "INTEGER" }, { "MAXRESPONSETIME", "DOUBLE" } };
	private static final List<String> ENGINEPERMISSION_COLUMNS = List.of("ENGINEID", "USERID", "PERMISSION",
			"USAGERESTRICTION", "USAGEFREQUENCY", "MAXTOKENS", "MAXRESPONSETIME");
	private static final String INSERT_GRANT = "INSERT INTO ENGINEPERMISSION (USERID, PERMISSION, ENGINEID, "
			+ "USAGERESTRICTION, USAGEFREQUENCY, MAXTOKENS) VALUES (?,?,?,?,?,?)";

	private IRDBMSEngine securityDb;
	private List<String> tables = new ArrayList<>();

	@BeforeEach
	void setup() throws Exception {
		securityDb = SystemEngineRegistry.getSecurityDb();
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		for (String[] column : ENGINE_LIMIT_COLUMNS) {
			securityDb.insertData(
					securityDb.getQueryUtil().alterTableAddColumn("ENGINEPERMISSION", column[0], column[1]));
		}
	}

	@AfterEach
	void cleanup() throws Exception {
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		for (String[] column : ENGINE_LIMIT_COLUMNS) {
			securityDb.insertData("ALTER TABLE ENGINEPERMISSION DROP COLUMN IF EXISTS " + column[0]);
		}
		tables = UnitTestSecurityAuthUtils.clearSecurityDB(securityDb, tables);
	}

	@Test
	void copyLimits_runTwiceLeavesOneLimitPerGrantMarkedAsCopied() throws Exception {
		execute(INSERT_GRANT, "alice", 3, "model", "token", "DAY", 5000);

		assertTrue(ModelUsageLimitColumnMigration.copyLimits(securityDb, "ENGINEPERMISSION", ENGINEPERMISSION_COLUMNS));
		// a second server starting at the same time runs the copy again
		assertTrue(ModelUsageLimitColumnMigration.copyLimits(securityDb, "ENGINEPERMISSION", ENGINEPERMISSION_COLUMNS));

		assertEquals(1L, count("SELECT COUNT(*) FROM MODELUSAGELIMIT WHERE SUBJECTID = 'alice'"));
		assertEquals(1L, count("SELECT COUNT(*) FROM MODELUSAGELIMITRULE"));
		assertEquals(ModelUsageLimitColumnMigration.COPIED_BY_ID,
				UnitTestGroupTables.queryValue(securityDb, "SELECT MODIFIEDBYID FROM MODELUSAGELIMIT"));
		assertEquals(5000L, amountOf(ModelUsageLimitKey.direct("model", "alice", null)));
	}

	@Test
	void copyLimits_keepsALimitSomeoneChangedAndCopiesAgainOneNobodyChanged() throws Exception {
		execute(INSERT_GRANT, "alice", 3, "model", "token", "DAY", 5000);
		execute(INSERT_GRANT, "bob", 3, "model", "token", "DAY", 6000);
		ModelUsageLimitColumnMigration.copyLimits(securityDb, "ENGINEPERMISSION", ENGINEPERMISSION_COLUMNS);

		// an owner changes alice's limit after the copy
		QueryExecutionUtility.write(securityDb, connection -> {
			SecurityModelUsageLimitUtils.setLimit(connection, ModelUsageLimitKey.direct("model", "alice", null),
					tokensADay(9000), Pair.with("owner", "NATIVE"));
			return null;
		});
		// and a server still on the four columns writes new amounts
		execute("UPDATE ENGINEPERMISSION SET MAXTOKENS = 7000 WHERE USERID = 'alice'");
		execute("UPDATE ENGINEPERMISSION SET MAXTOKENS = 8000 WHERE USERID = 'bob'");

		ModelUsageLimitColumnMigration.copyLimits(securityDb, "ENGINEPERMISSION", ENGINEPERMISSION_COLUMNS);

		assertEquals(9000L, amountOf(ModelUsageLimitKey.direct("model", "alice", null)));
		assertEquals(8000L, amountOf(ModelUsageLimitKey.direct("model", "bob", null)));
		assertEquals(2L, count("SELECT COUNT(*) FROM MODELUSAGELIMIT"));
	}

	@Test
	void copyLimits_leavesTheLimitsOfOtherSourcesAlone() throws Exception {
		execute(INSERT_GRANT, "alice", 3, "model", "token", "DAY", 5000);
		// an all-models default set by an admin, and a team's limit
		QueryExecutionUtility.write(securityDb, connection -> {
			SecurityModelUsageLimitUtils.setLimit(connection, ModelUsageLimitKey.allModels("alice", "NATIVE"),
					tokensADay(100), Pair.with("admin", "NATIVE"));
			SecurityModelUsageLimitUtils.setLimit(connection, ModelUsageLimitKey.team("model", "data", "CUSTOM"),
					tokensADay(200), Pair.with("owner", "NATIVE"));
			return null;
		});

		ModelUsageLimitColumnMigration.copyLimits(securityDb, "ENGINEPERMISSION", ENGINEPERMISSION_COLUMNS);
		ModelUsageLimitColumnMigration.copyLimits(securityDb, "ENGINEPERMISSION", ENGINEPERMISSION_COLUMNS);

		assertEquals(100L, amountOf(ModelUsageLimitKey.allModels("alice", "NATIVE")));
		assertEquals(200L, amountOf(ModelUsageLimitKey.team("model", "data", "CUSTOM")));
		assertEquals(3L, count("SELECT COUNT(*) FROM MODELUSAGELIMIT"));
	}

	@Test
	void readLimit_readsOneRuleForOnePerson() {
		ModelUsageLimit limit = ModelUsageLimitColumnMigration.readLimit("TOKEN", "week", 5000, null);

		assertFalse(limit.isUnlimited());
		assertEquals(1, limit.getRules().size());
		ModelUsageLimitRule rule = limit.getRules().get(0);
		assertEquals(Metric.TOKENS, rule.getMetric());
		assertEquals(5000L, rule.getAmount());
		assertEquals(Window.WEEK, rule.getWindow());
		assertEquals(Counts.EACH_PERSON, rule.getCounts());
	}

	@Test
	void readLimit_allTimeIsAYearAndBlankOrUnknownIsADay() {
		assertEquals(Window.YEAR,
				ModelUsageLimitColumnMigration.readLimit("token", "ALL_TIME", 10, null).getRules().get(0).getWindow());
		assertEquals(Window.DAY,
				ModelUsageLimitColumnMigration.readLimit("token", " ", 10, null).getRules().get(0).getWindow());
		assertEquals(Window.DAY,
				ModelUsageLimitColumnMigration.readLimit("token", "FORTNIGHT", 10, null).getRules().get(0).getWindow());
		assertEquals(Window.DAY, ModelUsageLimitColumnMigration.windowOf(null));
		assertEquals(Window.MONTH, ModelUsageLimitColumnMigration.windowOf(" month "));
	}

	@Test
	void readLimit_zeroOrNoAmountMeansNoLimitDefined() {
		assertNull(ModelUsageLimitColumnMigration.readLimit("token", "DAY", 0, null));
		assertNull(ModelUsageLimitColumnMigration.readLimit("token", "DAY", null, 500));
		assertNull(ModelUsageLimitColumnMigration.readLimit("compute", "DAY", null, 0.0));
		assertNull(ModelUsageLimitColumnMigration.readLimit(null, "DAY", 100, null));
		assertNull(ModelUsageLimitColumnMigration.readLimit("", "DAY", 100, null));
	}

	@Test
	void readLimit_computeTimeIsRoundedMilliseconds() {
		ModelUsageLimitRule rule = ModelUsageLimitColumnMigration.readLimit("compute", "MONTH", null, 1500.6).getRules()
				.get(0);

		assertEquals(Metric.COMPUTE_TIME, rule.getMetric());
		assertEquals(1501L, rule.getAmount());
	}

	@Test
	void readLimit_refusesAnUnknownRestriction() {
		assertThrows(IllegalArgumentException.class,
				() -> ModelUsageLimitColumnMigration.readLimit("requests", "DAY", 10, null));
	}

	private void execute(String sql, Object... values) throws Exception {
		QueryExecutionUtility.executeUpdate(securityDb, sql, ps -> {
			for (int i = 0; i < values.length; i++) {
				ps.setObject(i + 1, values[i]);
			}
		});
	}

	private long count(String sql) throws SQLException {
		return ((Number) UnitTestGroupTables.queryValue(securityDb, sql)).longValue();
	}

	private static long amountOf(ModelUsageLimitKey key) {
		return SecurityModelUsageLimitUtils.getLimit(key).getRules().get(0).getAmount();
	}

	private static ModelUsageLimit tokensADay(long amount) {
		return ModelUsageLimit
				.of(List.of(new ModelUsageLimitRule(Metric.TOKENS, amount, Window.DAY, Counts.EACH_PERSON)));
	}
}
