package prerna.reactor.frame.py;

import java.io.File;
import java.io.IOException;

import org.apache.logging.log4j.Logger;

import prerna.ds.py.PyUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class PokeReactor extends AbstractReactor {
	
	private static transient SecurityManager defaultManager = System.getSecurityManager();
	private static final String CLASS_NAME = PokeReactor.class.getName();
	
	// basically takes the insight and drops a .completed file
	// to trigger a waiting process for something
	
	@Override
	public NounMetadata execute() {
		if(!PyUtils.pyEnabled()) {
			throw new IllegalArgumentException("Python is not enabled to use the following command");
		}
		Logger logger = getLogger(CLASS_NAME);

		String fileName = insight.getTupleSpace() + "/poke.completed";
		File file = new File(fileName);
		String parent = file.getParentFile().getParent();
		if(file.exists())
			file.delete();
		
		// now close everything
		File closeAllFile = new File(parent + "/alldone.closeall");
		
		try {
			file.createNewFile();
			closeAllFile.createNewFile();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return new NounMetadata("Poke Complete", PixelDataType.CODE, PixelOperationType.CODE_EXECUTION);
	}

}
