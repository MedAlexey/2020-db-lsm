package ru.mail.polis.medAlexey;

import org.jetbrains.annotations.NotNull;

import java.util.Optional;
import java.nio.ByteBuffer;

final class Value implements Comparable<Value> {

    @NotNull
    private final Optional<ByteBuffer> data;
    private final long timestamp;

    Value(final long timestamp, @NotNull final ByteBuffer value) {
        assert timestamp > 0L;
        this.timestamp = timestamp;
        this.data = Optional.of(value);
    }

    Value(final long timestamp) {
        assert timestamp > 0L;
        this.timestamp = timestamp;
        this.data = Optional.empty();
    }

    boolean isTombstone() {
        return data.isEmpty();
    }

    @NotNull
    ByteBuffer getData() {
        assert !isTombstone();
        return data.orElseThrow().asReadOnlyBuffer() ;
    }

    @Override
    public int compareTo(@NotNull final Value o) {
        return -Long.compare(timestamp, o.timestamp);
    }

    public long getTimestamp() {
        return timestamp;
    }
}
