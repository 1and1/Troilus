package net.oneandone.troilus;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import com.datastax.driver.core.TypeCodec;
import com.google.common.reflect.TypeToken;
import com.google.common.reflect.TypeParameter;;


/**
 * copied from com.datastax.driver.extras.codecs.jdk8.OptionalCodec 
 */ 
class OptionalCodec<T> extends MappingCodec<Optional<T>, T> {

    private final Predicate<T> isAbsent;

    public OptionalCodec(TypeCodec<T> codec) {
        this(codec, new Predicate<T>() {
            @Override
            public boolean test(T input) {
                return input == null
                    || input instanceof Collection && ((Collection<?>)input).isEmpty()
                    || input instanceof Map && ((Map<?, ?>) input).isEmpty();

            }
        });
    }
    
    @SuppressWarnings("serial")
    public OptionalCodec(TypeCodec<T> codec, Predicate<T> isAbsent) {
        super(codec, new TypeToken<Optional<T>>(){}.where(new TypeParameter<T>(){}, codec.getJavaType()));
        this.isAbsent = isAbsent;
    }

    @Override
    protected Optional<T> deserialize(T value) {
        return isAbsent(value) ? Optional.<T>empty() : Optional.of(value);
    }

    @Override
    protected T serialize(Optional<T> value) {
        return value.isPresent() ? value.get() : absentValue();
    }

    protected T absentValue() {
        return null;
    }

    protected boolean isAbsent(T value) {
        return isAbsent.test(value);
    }
}
