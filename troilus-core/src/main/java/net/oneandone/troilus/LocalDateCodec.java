package net.oneandone.troilus;

import static com.datastax.driver.core.CodecUtils.fromCqlDateToDaysSinceEpoch;
import static com.datastax.driver.core.CodecUtils.fromSignedToUnsignedInt;
import static com.datastax.driver.core.CodecUtils.fromUnsignedToSignedInt;
import static com.datastax.driver.core.ParseUtils.isLongLiteral;
import static com.datastax.driver.core.ParseUtils.isQuoted;
import static com.datastax.driver.core.ParseUtils.quote;
import static com.datastax.driver.core.ParseUtils.unquote;
import static java.lang.Long.parseLong;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;


/**
 * copied from com.datastax.driver.extras.codecs.jdk8.InstantCodec 
 */ 
class LocalDateCodec extends TypeCodec<LocalDate> {

    public static final LocalDateCodec instance = new LocalDateCodec();

    private static final LocalDate EPOCH = LocalDate.of(1970, 1, 1);

    private LocalDateCodec() {
        super(DataType.date(), LocalDate.class);
    }

    @Override
    public ByteBuffer serialize(LocalDate value, ProtocolVersion protocolVersion) {
        if (value == null)
            return null;
        long days = ChronoUnit.DAYS.between(EPOCH, value);
        int unsigned = fromSignedToUnsignedInt((int) days);
        return cint().serializeNoBoxing(unsigned, protocolVersion);
    }

    @Override
    public LocalDate deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        if (bytes == null || bytes.remaining() == 0)
            return null;
        int unsigned = cint().deserializeNoBoxing(bytes, protocolVersion);
        int signed = fromUnsignedToSignedInt(unsigned);
        return EPOCH.plusDays(signed);
    }

    @Override
    public String format(LocalDate value) {
        if (value == null)
            return "NULL";
        return quote(ISO_LOCAL_DATE.format(value));
    }

    @Override
    public LocalDate parse(String value) {
        if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL"))
            return null;

        // single quotes are optional for long literals, mandatory for date patterns
        // strip enclosing single quotes, if any
        if (isQuoted(value))
            value = unquote(value);

        if (isLongLiteral(value)) {
            long raw;
            try {
                raw = parseLong(value);
            } catch (NumberFormatException e) {
                throw new InvalidTypeException(String.format("Cannot parse date value from \"%s\"", value));
            }
            int days;
            try {
                days = fromCqlDateToDaysSinceEpoch(raw);
            } catch (IllegalArgumentException e) {
                throw new InvalidTypeException(String.format("Cannot parse date value from \"%s\"", value));
            }
            return EPOCH.plusDays(days);
        }

        try {
            return LocalDate.parse(value, ISO_LOCAL_DATE);
        } catch (RuntimeException e) {
            throw new InvalidTypeException(String.format("Cannot parse date value from \"%s\"", value));
        }
    }

}
