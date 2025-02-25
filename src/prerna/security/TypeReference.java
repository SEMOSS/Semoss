package prerna.security;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public abstract class TypeReference<T> {
	private final Type type;

	protected TypeReference() {
		Type superclass = getClass().getGenericSuperclass();
		if (superclass instanceof ParameterizedType) {
			this.type = ((ParameterizedType) superclass).getActualTypeArguments()[0];
		} else {
			throw new IllegalArgumentException("TypeReference must be created with a generic type.");
		}
	}

	public Type getType() {
		return this.type;
	}
}
