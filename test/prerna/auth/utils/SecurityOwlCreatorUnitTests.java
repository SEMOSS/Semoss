package prerna.auth.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.engine.api.IRDBMSEngine;

public class SecurityOwlCreatorUnitTests extends AbstractSecurityUtilsUnitTestsSetup {

	private IRDBMSEngine securityDb;
	private List<String> tables = new ArrayList<>();

	private SecurityOwlCreator creator;

	@BeforeEach
	void setup() {
		securityDb = AbstractSecurityUtils.securityDb;
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		assertNotNull(this.securityDb);

		creator = new SecurityOwlCreator(securityDb);
	}

	@AfterEach
	void cleanup() throws SQLException {
		assertTrue(securityDb.getOwlFilePath().contains("junit"));
		// clear test database inside of temp directory
		// quicker than deleting and recreating
		tables = UnitTestSecurityAuthUtils.clearSecurityDB(securityDb, tables);
	}

	///
	/// needsRemake
	///

	// This method always remakes?
	@Test
	void testNeedsRemake() {
		assertTrue(creator.needsRemake());
	}

	///
	/// remakeOwl
	///
	@Test
	void testRemakeOwl_successful() throws Exception {
		Files.delete(securityOwlFile);

		creator.remakeOwl();

		assertTrue(Files.exists(securityOwlFile));
		try (Stream<String> lines = Files.lines(securityOwlFile)) {
			// this number will change with changes to security db schema
			assertEquals(4404, lines.count());
		}
	}

}
