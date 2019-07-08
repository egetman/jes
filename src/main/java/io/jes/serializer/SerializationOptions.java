package io.jes.serializer;

@SuppressWarnings("unused")
public enum SerializationOptions implements SerializationOption {

    /**
     * This property informs target serializer, that it should store provided type aliases instead of plain class
     * names (via {@link TypeRegistry}). It can be useful to avoid {@link ClassNotFoundException} & etc.
     */
    USE_TYPE_ALIASES

}
