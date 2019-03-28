package prerna.util;

import java.util.function.Consumer;

import org.apache.tinkerpop.gremlin.structure.io.Mapper;

public class MyGraphIoMappingBuilder implements Consumer<Mapper.Builder> {
	
	@Override
	public void accept(Mapper.Builder t) {
		t.addRegistry(new MyGraphIoRegistry());
	}

}