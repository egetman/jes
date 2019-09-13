package store.jesframework.serializer;

import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.NoArgsConstructor;
import store.jesframework.Event;
import store.jesframework.serializer.api.EventSerializer;

@NoArgsConstructor
class JacksonEventSerializer extends JacksonSerializer<Event> implements EventSerializer<String> {

    /**
     * A little hack to faster resolve type name. All json events (for now) starts with {"@type":"
     */
    private static final int START_TYPE_NAME_POSITION = 10;
    static final int DEFAULT_NAME_SIZE = 60;

    @SuppressWarnings("WeakerAccess")
    public JacksonEventSerializer(@Nullable TypeRegistry registry) {
        super(new ObjectMapper(), registry);
    }

    @SuppressWarnings("unused")
    public JacksonEventSerializer(@Nonnull ObjectMapper mapper, @Nullable TypeRegistry registry) {
        super(mapper, registry);
    }

    @Nullable
    @Override
    public String fetchTypeName(@Nonnull String raw) {
        Objects.requireNonNull(raw, "Raw event must not be null");
        int size = 0;
        final char[] searched = raw.toCharArray();
        char[] typeNameArray = new char[DEFAULT_NAME_SIZE];

        for (int i = START_TYPE_NAME_POSITION; i < searched.length; i++) {
            if (searched[i] != '"') {
                typeNameArray[size++] = searched[i];
                if (size == typeNameArray.length) {
                    final char[] temp = new char[typeNameArray.length * 2];
                    System.arraycopy(typeNameArray, 0, temp, 0, typeNameArray.length);
                    typeNameArray = temp;
                }
            } else {
                break;
            }
        }
        return new String(typeNameArray, 0, size);
    }

}
