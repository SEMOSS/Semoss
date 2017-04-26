package prerna.sablecc2;

import java.util.*;

import prerna.sablecc2.analysis.DepthFirstAdapter;
import prerna.sablecc2.node.AFrameop;
import prerna.sablecc2.node.AOperationFormula;
import prerna.sablecc2.reactor.ReactorFactory;

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
	
	public void inAOperationFormula(AOperationFormula node) {
		String reactorId = node.getId().toString().trim();
		boolean isImplemented = ReactorFactory.hasReactor(reactorId);
		implementedReactorChecks.get(isImplemented).add(reactorId);
		
	}
	
	public void inAFrameop(AFrameop node) {
		String reactorId = node.getId().toString().trim();
		boolean isImplemented = ReactorFactory.hasReactor(reactorId);
		implementedReactorChecks.get(isImplemented).add(reactorId);
	}
}
