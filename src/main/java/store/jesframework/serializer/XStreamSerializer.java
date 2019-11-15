package store.jesframework.serializer;

import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.Xpp3Driver;
import com.thoughtworks.xstream.mapper.CannotResolveClassException;
import com.thoughtworks.xstream.security.AnyTypePermission;

import store.jesframework.ex.SerializationException;
import store.jesframework.serializer.api.Format;
import store.jesframework.serializer.api.Serializer;

class XStreamSerializer<S> implements Serializer<S, String> {

    // it's thread-safe, so it's ok to have just 1 instance
    private final XStream xstream = new XStream(new Xpp3Driver());

    XStreamSerializer() {
        this(null);
    }

    XStreamSerializer(@Nullable TypeRegistry typeRegistry) {
        XStream.setupDefaultSecurity(xstream);
        xstream.addPermission(AnyTypePermission.ANY);

        if (typeRegistry != null) {
            final Map<Class<?>, String> aliases = typeRegistry.getAliases();
            aliases.forEach((clazz, name) -> xstream.alias(name, clazz));
        }
    }

    @Nonnull
    @Override
    public String serialize(@Nonnull S toSerialize) {
        return xstream.toXML(toSerialize);
    }

    @Nonnull
    @Override
    public S deserialize(@Nonnull String toDeserialize) {
        try {
            //noinspection unchecked
            return (S) xstream.fromXML(toDeserialize);
        } catch (CannotResolveClassException e) {
            throw new TypeNotPresentException(e.getMessage(), e);
        } catch (Exception e) {
            throw new SerializationException(e);
        }
    }

    @Nonnull
    @Override
    public Format format() {
        return Format.XML_XSTREAM;
    }
}
