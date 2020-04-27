package prerna.sablecc2.reactor.frame.py;

import java.util.List;
import java.util.Vector;

import org.apache.log4j.Logger;

import prerna.ds.py.PyTranslator;
import prerna.ds.py.PyUtils;
import prerna.ds.py.TCPPyTranslator;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Utility;

public class PyReactor extends AbstractReactor {
	
	private static transient SecurityManager defaultManager = System.getSecurityManager();
	private static final String CLASS_NAME = PyReactor.class.getName();
	
	@Override
	public NounMetadata execute() {
		if(!PyUtils.pyEnabled()) {
			throw new IllegalArgumentException("Python is not enabled to use the following command");
		}
		Logger logger = getLogger(CLASS_NAME);

		String code = Utility.decodeURIComponent(this.curRow.get(0).toString());

		PyTranslator pyTranslator = this.insight.getPyTranslator();
		pyTranslator.setLogger(logger);
		//String output = pyTranslator.runPyAndReturnOutput(code);
		String output = null;
		/*
		if(pyTranslator instanceof TCPPyTranslator)
		{
			output = ((TCPPyTranslator)pyTranslator).runScript(code) + "";
			System.out.println("OUTPUT" + output);
		}
		else*/
			output = pyTranslator.runPyAndReturnOutput(insight.getUser().getAppMap(), code) + "";
		
		List<NounMetadata> outputs = new Vector<NounMetadata>(1);
		outputs.add(new NounMetadata(output, PixelDataType.CONST_STRING));
		return new NounMetadata(outputs, PixelDataType.CODE, PixelOperationType.CODE_EXECUTION);
	}

}
