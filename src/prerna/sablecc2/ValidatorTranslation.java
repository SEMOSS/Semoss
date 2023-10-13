package prerna.sablecc2;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import prerna.reactor.ReactorFactory;
import prerna.sablecc2.analysis.DepthFirstAdapter;
import prerna.sablecc2.node.AOperation;

public class ValidatorTranslation extends DepthFirstAdapter {

	private Map<Boolean, Set<String>> implementedReactorChecks;
	
	public ValidatorTranslation() {
		implementedReactorChecks = new HashMap<>(2);
		implementedReactorChecks.put(true, new HashSet<>());
		implementedReactorChecks.put(false, new HashSet<>());
	}
	
	public Set<String> getUnimplementedReactors() {
		return implementedReactorChecks.get(false);
	}
	
	@Override
	public void inAOperation(AOperation node) {
		String reactorId = node.getId().toString().trim();
		boolean isImplemented = ReactorFactory.hasReactor(reactorId);
		implementedReactorChecks.get(isImplemented).add(reactorId);
	}
}
