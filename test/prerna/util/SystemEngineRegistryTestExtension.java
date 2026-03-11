package prerna.util;

import java.lang.reflect.Field;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * JUnit 5 extension that clears the SecurityDb registration in
 * {@link SystemEngineRegistry} after each test class completes. This allows
 * each test class that extends AbstractSecurityUtilsUnitTestsSetup to register
 * its own fresh in-memory H2 database without hitting "already registered".
 *
 * Lives in the test source tree only — not shipped in production JARs. Uses
 * reflection to null the private {@code securityDbHolder} field so that no
 * test-related code needs to exist in the production class.
 */
public class SystemEngineRegistryTestExtension implements AfterAllCallback {

	@Override
	public void afterAll(ExtensionContext context) throws Exception {
		Field field = SystemEngineRegistry.class.getDeclaredField("securityDbHolder");
		field.setAccessible(true);
		field.set(null, null);
	}

}
