package store.jesframework.serializer;

import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.Xpp3Driver;

import store.jesframework.serializer.api.Serializer;

public class XStreamSerializer<S> implements Serializer<S, String> {

    private final XStream xstream = new XStream(new Xpp3Driver());

    public XStreamSerializer(@Nullable TypeRegistry typeRegistry) {
        if (typeRegistry == null) {
            return;
        }
        final Map<Class<?>, String> aliases = typeRegistry.getAliases();
        aliases.forEach((clazz, name) -> xstream.alias(name, clazz));
    }

    @Nonnull
    @Override
    public String serialize(@Nonnull S toSerialize) {
        return xstream.toXML(toSerialize);
    }

    @Nonnull
    @Override
    public S deserialize(@Nonnull String toDeserialize) {
        //noinspection unchecked
        return (S) xstream.fromXML(toDeserialize);
    }
}
