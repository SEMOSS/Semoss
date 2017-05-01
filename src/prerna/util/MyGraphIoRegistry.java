package prerna.util;

import java.util.Vector;

import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;

import prerna.algorithm.api.IMetaData;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.reactor.AssignmentReactor;
import prerna.sablecc2.reactor.IReactor;

public class MyGraphIoRegistry extends AbstractIoRegistry {
    public MyGraphIoRegistry() {
        register(GryoIo.class, Vector.class, null);
        register(GryoIo.class, IMetaData.NAME_TYPE.class, null);
        register(GryoIo.class, IMetaData.DATA_TYPES.class, null);
        register(GryoIo.class, IReactor.TYPE.class, null);
        register(GryoIo.class, AssignmentReactor.class, null);
        register(GryoIo.class, GenRowStruct.class, null);
        register(GryoIo.class, NounMetadata.class, null);
        register(GryoIo.class, PkslDataTypes.class, null);
        
    }
}