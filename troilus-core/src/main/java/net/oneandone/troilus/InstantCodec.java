package net.oneandone.troilus;

import static com.datastax.driver.core.ParseUtils.isLongLiteral;
import static com.datastax.driver.core.ParseUtils.isQuoted;
import static com.datastax.driver.core.ParseUtils.quote;
import static com.datastax.driver.core.ParseUtils.unquote;
import static java.lang.Long.parseLong;
import static java.time.Instant.ofEpochMilli;
import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;

/**
 * copied from com.datastax.driver.extras.codecs.jdk8.InstantCodec 
 */ 
class InstantCodec extends TypeCodec<Instant> {

    public static final InstantCodec instance = new InstantCodec();

    /**
     * A {@link DateTimeFormatter} that parses (most) of
     * the ISO formats accepted in CQL.
     */
    private static final DateTimeFormatter PARSER = new DateTimeFormatterBuilder()
        .parseCaseSensitive()
        .parseStrict()
        .append(DateTimeFormatter.ISO_LOCAL_DATE)
        .optionalStart()
        .appendLiteral('T')
        .appendValue(HOUR_OF_DAY, 2)
        .appendLiteral(':')
        .appendValue(MINUTE_OF_HOUR, 2)
        .optionalEnd()
        .optionalStart()
        .appendLiteral(':')
        .appendValue(SECOND_OF_MINUTE, 2)
        .optionalEnd()
        .optionalStart()
        .appendFraction(NANO_OF_SECOND, 0, 9, true)
        .optionalEnd()
        .optionalStart()
        .appendZoneId()
        .optionalEnd()
        .toFormatter()
        .withZone(UTC);

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSxxx")
        .withZone(UTC);


    private InstantCodec() {
        super(DataType.timestamp(), Instant.class);
    }

    @Override
    public ByteBuffer serialize(Instant value, ProtocolVersion protocolVersion) {
        return value == null ? null : bigint().serializeNoBoxing(value.toEpochMilli(), protocolVersion);
    }

    @Override
    public Instant deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        return bytes == null || bytes.remaining() == 0 ? null : ofEpochMilli(bigint().deserializeNoBoxing(bytes, protocolVersion));
    }

    @Override
    public String format(Instant value) {
        if (value == null)
            return "NULL";
        return quote(FORMATTER.format(value));
    }

    @Override
    public Instant parse(String value) {
        if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL"))
            return null;
        // strip enclosing single quotes, if any
        if (isQuoted(value))
            value = unquote(value);
        if (isLongLiteral(value)) {
            try {
                return ofEpochMilli(parseLong(value));
            } catch (NumberFormatException e) {
                throw new InvalidTypeException(String.format("Cannot parse timestamp value from \"%s\"", value));
            }
        }
        try {
            return Instant.from(PARSER.parse(value));
        } catch (DateTimeParseException e) {
            throw new InvalidTypeException(String.format("Cannot parse timestamp value from \"%s\"", value));
        }
    }

}