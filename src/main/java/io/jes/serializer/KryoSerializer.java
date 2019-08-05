package io.jes.serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.DefaultInstantiatorStrategy;

import org.objenesis.strategy.StdInstantiatorStrategy;

import io.jes.ex.SerializationException;

class KryoSerializer<S> implements Serializer<S, byte[]> {

    private static final String NO_CLASS_KRYO_MESSAGE = "Unable to find class: ";
    private final ThreadLocal<Kryo> kryo = ThreadLocal.withInitial(() -> {
        final Kryo serializer = new Kryo();
        serializer.setRegistrationRequired(false);
        serializer.setInstantiatorStrategy(new DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
        return serializer;
    });

    @Override
    public byte[] serialize(S toSerialize) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); Output output = new Output(baos)) {
            kryo.get().writeClassAndObject(output, toSerialize);
            output.flush();
            return baos.toByteArray();
        } catch (Exception e) {
            throw new SerializationException(e);
        }
    }

    @Override
    public S deserialize(byte[] toDeserialize) {
        if (toDeserialize == null || toDeserialize.length == 0) {
            throw new SerializationException("Can't deserialize event from 0 length binary uuid");
        }
        try (final ByteArrayInputStream bais = new ByteArrayInputStream(toDeserialize);
             final Input input = new Input(bais, toDeserialize.length)) {

            //noinspection unchecked
            return (S) kryo.get().readClassAndObject(input);
        } catch (KryoException e) {
            final String message = e.getMessage();
            if (message != null && message.contains(NO_CLASS_KRYO_MESSAGE)) {
                final String typeName = message.substring(NO_CLASS_KRYO_MESSAGE.length());
                throw new TypeNotPresentException(typeName, e);
            }
            throw new SerializationException(e);
        } catch (Exception e) {
            throw new SerializationException(e);
        }
    }
}
