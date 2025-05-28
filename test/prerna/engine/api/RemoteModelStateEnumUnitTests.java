package prerna.engine.api;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Vector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections4.CollectionUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import prerna.engine.api.RemoteModelStateEnum;

public class RemoteModelStateEnumUnitTests {
	private RemoteModelStateEnum[] enumArr;
	
	@BeforeEach
	void setUp() {
		enumArr = RemoteModelStateEnum.values();
	}
	
	@Test
	void testAllEnumsExist() {
		List<RemoteModelStateEnum> enumLst = Stream.of(
				RemoteModelStateEnum.COLD,
				RemoteModelStateEnum.WARMING,
				RemoteModelStateEnum.ACTIVE,
				RemoteModelStateEnum.FAILED,
				RemoteModelStateEnum.UNKNOWN
				).collect(Collectors.toCollection(Vector::new));
		
		assertTrue(CollectionUtils.containsAll(enumLst, Arrays.asList(enumArr)));
	}
	
}
