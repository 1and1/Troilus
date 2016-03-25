package net.oneandone.troilus;
import java.nio.ByteBuffer;


import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.google.common.reflect.TypeToken;


/**
 * copied from com.datastax.driver.extras.codecs.MappingCodec 
 */ 
abstract class MappingCodec<O, I> extends TypeCodec<O> {

    protected final TypeCodec<I> innerCodec;

    public MappingCodec(TypeCodec<I> innerCodec, Class<O> javaType) {
        this(innerCodec, TypeToken.of(javaType));
    }

    public MappingCodec(TypeCodec<I> innerCodec, TypeToken<O> javaType) {
        super(innerCodec.getCqlType(), javaType);
        this.innerCodec = innerCodec;
    }

    @Override
    public ByteBuffer serialize(O value, ProtocolVersion protocolVersion) throws InvalidTypeException {
        return innerCodec.serialize(serialize(value), protocolVersion);
    }

    @Override
    public O deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
        return deserialize(innerCodec.deserialize(bytes, protocolVersion));
    }

    @Override
    public O parse(String value) throws InvalidTypeException {
        return value == null || value.isEmpty() || value.equalsIgnoreCase("NULL") ? null : deserialize(innerCodec.parse(value));
    }

    @Override
    public String format(O value) throws InvalidTypeException {
        return value == null ? null : innerCodec.format(serialize(value));
    }

    protected abstract O deserialize(I value);

    protected abstract I serialize(O value);
}
