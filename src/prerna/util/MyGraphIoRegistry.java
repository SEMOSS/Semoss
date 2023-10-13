package prerna.util;

import java.util.Vector;

import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;

import prerna.algorithm.api.SemossDataType;
import prerna.reactor.AssignmentReactor;
import prerna.reactor.IReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class MyGraphIoRegistry extends AbstractIoRegistry {
	
    public MyGraphIoRegistry() {
        register(GryoIo.class, Vector.class, null);
        register(GryoIo.class, SemossDataType.class, null);
        register(GryoIo.class, IReactor.TYPE.class, null);
        register(GryoIo.class, AssignmentReactor.class, null);
        register(GryoIo.class, GenRowStruct.class, null);
        register(GryoIo.class, NounMetadata.class, null);
        register(GryoIo.class, PixelDataType.class, null);
    }
}