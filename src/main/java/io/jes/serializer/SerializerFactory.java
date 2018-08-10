package io.jes.serializer;

import javax.annotation.Nonnull;

public class SerializerFactory {

    @Nonnull
    public static EventSerializer<byte[]> binarySerializer() {
        return new KryoBinaryEventSerializer();
    }

}
