package ru.mail.polis.medAlexey;

import org.jetbrains.annotations.NotNull;

import java.util.TreeMap;
import java.util.Iterator;
import java.nio.ByteBuffer;
import java.util.SortedMap;

final class MemTable implements Table{
    private final SortedMap<ByteBuffer, Value> map = new TreeMap<>();
    private long sizeInBytes = 0L;

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) {
        return map.tailMap(from)
                .entrySet()
                .stream()
                .map(element -> new Cell(element.getKey(), element.getValue()))
                .iterator();
    }

    @Override
    public void upsert(
            @NotNull final ByteBuffer key,
            @NotNull final ByteBuffer value) {
        final Value oldValue = map.put(key, new Value(System.currentTimeMillis(), value));

        if (oldValue == null) {
            sizeInBytes += key.remaining() + value.remaining();
        } else if (oldValue.isTombstone()) {
            sizeInBytes += value.remaining();
        } else {
            sizeInBytes += value.remaining() - oldValue.getData().remaining();
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        final Value oldValue = map.put(key, new Value(System.currentTimeMillis()));

        if (oldValue == null) {
            sizeInBytes += key.remaining();
        } else if (!oldValue.isTombstone()) {
            sizeInBytes -= oldValue.getData().remaining();
        }
    }

    @Override
    public long sizeInBytes() {
        return sizeInBytes;
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public void close() {

    }
}
