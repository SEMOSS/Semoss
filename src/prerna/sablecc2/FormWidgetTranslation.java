package prerna.sablecc2;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.sablecc2.analysis.DepthFirstAdapter;
import prerna.sablecc2.node.AGeneric;
import prerna.sablecc2.node.AOperation;
import prerna.sablecc2.node.ARoutineConfiguration;
import prerna.sablecc2.node.AScalarRegTerm;
import prerna.sablecc2.node.PRoutine;

public class FormWidgetTranslation extends DepthFirstAdapter {

	private static final Logger LOGGER = LogManager.getLogger(DashboardRecipeTranslation.class.getName());

	private List<String> into = new Vector<String>();
	private List<String> values = new Vector<String>();

	private boolean isInsert = false;
	private boolean isInto = false;
	private boolean isValues = false;

	@Override
	public void caseARoutineConfiguration(ARoutineConfiguration node) {
		List<PRoutine> copy = new ArrayList<PRoutine>(node.getRoutine());
		for (PRoutine e : copy) {
//			String expression = e.toString();
//			LOGGER.info("Processing " + expression);
			e.apply(this);
		}
	}

	@Override
	public void inAOperation(AOperation node) {
		String reactorId = node.getId().toString().trim();
		if (reactorId.equals("Insert")) {
			isInsert = true;
		}
	}

	@Override
	public void outAOperation(AOperation node) {
		isInsert = false;
	}

	@Override
	public void inAGeneric(AGeneric node) {
		if (isInsert) {
			String id = node.getId().toString().trim();

			if (id.equals("into")) {
				isInto = true;
			} else if (id.equals("values")) {
				isValues = true;
			}
		}
	}

	@Override
	public void outAGeneric(AGeneric node) {
		isInto = false;
		isValues = false;
	}

	@Override
	public void inAScalarRegTerm(AScalarRegTerm node) {
		if (isInto) {
			into.add(node.toString().trim());
		} else if (isValues) {
			// clean up param string "<Param>" -> param
			values.add(node.toString().trim().replace("\"", "").replace("<", "").replace(">", ""));
		}
	}

	public List<String> getInto() {
		return into;
	}

	public List<String> getValues() {
		return values;
	}
}
