package store.jesframework.serializer.api;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public enum Format implements SerializationOption {

    /**
     * JSON_JACKSON is the default serialization format.
     */
    JSON_JACKSON(String.class),
    XML_XSTREAM(String.class),
    BINARY_KRYO(byte[].class);

    @Getter
    private final Class<?> javaType;

}
