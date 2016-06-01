package prerna.util;

import java.util.Vector;

import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;

import prerna.algorithm.api.IMetaData;

public class MyGraphIoRegistry extends AbstractIoRegistry {
    public MyGraphIoRegistry() {
        register(GryoIo.class, Vector.class, null);
        register(GryoIo.class, IMetaData.NAME_TYPE.class, null);
        register(GryoIo.class, IMetaData.DATA_TYPES.class, null);
    }
}