package prerna.engine.impl.function;

import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.BeforeEach;

import prerna.auth.User;
import prerna.om.Insight;

public class StreamRESTFunctionEngineUnitTests {
	private Insight insight;
	private User user;
	private StreamRESTFunctionEngine engine;

	@BeforeEach
	void setUp() {
		user = mock(User.class);
		// using inner class to access abstract methods
		engine = new StreamRESTFunctionEngine();
		insight = mock(Insight.class);
	}
}
