package prerna.sablecc2.reactor.masterdatabase;

import java.util.Vector;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

/**
 * Inputs for adding, updating and getting concept metadata
 *
 */
public abstract class AbstractMetaDBReactor extends AbstractReactor {

	private static final String ENGINE = "engine";
	private static final String CONCEPT = "concept";
	private static final String DESCRIPTION = "description";
	protected static final String VALUE_DELIMITER = ":::";
	private static final String VALUES = "values";

	/**
	 * Get engine name input from pixel
	 * 
	 * @return
	 */
	public String getEngine() {
		GenRowStruct engineGRS = this.store.getNoun(ENGINE);
		if (engineGRS != null) {
			NounMetadata noun = engineGRS.getNoun(0);
			if (noun != null) {
				return noun.getValue() + "";
			}
		}
		throw new IllegalArgumentException("Need to define the " + ENGINE + " to be updated");
	}

	/**
	 * Get concept input from pixel
	 * 
	 * @return
	 */
	public String getConcept() {
		GenRowStruct conceptGRS = this.store.getNoun(CONCEPT);
		if (conceptGRS != null) {
			NounMetadata noun = conceptGRS.getNoun(0);
			if (noun != null) {
				return noun.getValue() + "";
			}
		}
		throw new IllegalArgumentException("Need to define the " + CONCEPT + " to be updated");
	}

	/**
	 * Get description input from pixel
	 * 
	 * @return
	 */
	public String getDescription() {
		GenRowStruct descriptionGRS = this.store.getNoun(DESCRIPTION);
		if (descriptionGRS != null) {
			NounMetadata noun = descriptionGRS.getNoun(0);
			if (noun != null) {
				return noun.getValue() + "";
			}
		}
		throw new IllegalArgumentException("Need to define the " + DESCRIPTION + " to be updated");
	}

	/**
	 * Get values array as string from pixel input
	 * 
	 * @return
	 */
	public Vector<String> getValues() {
		GenRowStruct tagGRS = this.store.getNoun(VALUES);
		Vector<String> values = new Vector<String>();
		if (tagGRS != null) {
			for (int i = 0; i < tagGRS.size(); i++) {
				NounMetadata noun = tagGRS.getNoun(i);
				if (noun != null) {
					String value = noun.getValue() + "";
					values.add(value);
				}
			}
			return values;
		}
		throw new IllegalArgumentException("Need to define the " + VALUES + " to be updated");
	}

}
