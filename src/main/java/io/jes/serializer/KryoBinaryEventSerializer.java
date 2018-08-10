package io.jes.serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.DefaultInstantiatorStrategy;

import org.objenesis.strategy.StdInstantiatorStrategy;

import io.jes.Event;
import io.jes.ex.SerializationException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class KryoBinaryEventSerializer implements EventSerializer<byte[]> {

    private final Kryo kryo = new Kryo();

    KryoBinaryEventSerializer() {
        kryo.setRegistrationRequired(false);
        kryo.setInstantiatorStrategy(new DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
    }

    @Override
    public byte[] serialize(Event event) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); Output output = new Output(baos)) {
            kryo.writeClassAndObject(output, event);
            output.flush();
            return baos.toByteArray();
        } catch (Exception e) {
            throw new SerializationException(e);
        }
    }

    @Override
    public Event deserialize(byte[] event) {
        if (event == null || event.length == 0) {
            throw new SerializationException("Can't deserialize event from 0 length binary stream");
        }
        try (ByteArrayInputStream bais = new ByteArrayInputStream(event); Input input = new Input(bais, event.length)) {
            return (Event) kryo.readClassAndObject(input);
        } catch (Exception e) {
            throw new SerializationException(e);
        }
    }
}
