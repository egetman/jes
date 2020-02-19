package store.jesframework.serializer.impl;

import java.util.Map;
import javax.annotation.Nonnull;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.Xpp3Driver;
import com.thoughtworks.xstream.mapper.CannotResolveClassException;
import com.thoughtworks.xstream.security.AnyTypePermission;

import lombok.extern.slf4j.Slf4j;
import store.jesframework.ex.SerializationException;
import store.jesframework.serializer.api.Format;
import store.jesframework.serializer.api.Serializer;

import static java.util.Objects.requireNonNull;

@Slf4j
class XStreamSerializer<S> implements Serializer<S, String> {

    // it's thread-safe, so it's ok to have just 1 instance
    private final XStream xstream = new XStream(new Xpp3Driver());

    XStreamSerializer() {
        this(Context.parse());
    }

    XStreamSerializer(@Nonnull Context<?> context) {
        XStream.setupDefaultSecurity(xstream);
        xstream.addPermission(AnyTypePermission.ANY);
        final Map<Class<?>, String> aliases = requireNonNull(context, "Context must not be null").classesToAliases();
        log.debug("Prepared {} type alias(es)", aliases.size());
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
