package store.jesframework.util;


import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

/**
 * A convenience class to represent name-value pairs.
 *
 * @param <K> is a key of a pair.
 * @param <V> is a value of a pair.
 */
@Getter
@ToString
@EqualsAndHashCode
@RequiredArgsConstructor(staticName = "of")
public final class Pair<K, V> {

    private final K key;
    private final V value;

}
