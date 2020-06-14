package store.jesframework.serializer.api;

/**
 * Strategy to be used during serialization/deserialization. Information about the actual type will be stored based on
 * selected choice.
 */
public enum AliasingStrategy implements SerializationOption {

    /**
     * Store the type information about an object as full qualified class name. This strategy is the default one.
     */
    FULL_CLASS_NAME,

    /**
     * Store the type information about an object as short class name.
     */
    SHORT_CLASS_NAME

}
