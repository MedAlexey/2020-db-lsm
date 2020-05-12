package ru.mail.polis.medAlexey;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.util.Iterator;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

final class SSTable implements Table{

    private final FileChannel channel;
    private final long fileSize;
    private final int rows;

    SSTable(@NotNull final File file) throws IOException{
        this.channel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
        this.fileSize = channel.size();
        final ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES);
        channel.read(buf, this.fileSize - Integer.BYTES);
        this.rows = buf.rewind().getInt();
    }

    public static void serialize(
            final File file,
            final Iterator<Cell> cellIterator,
            final int rows) throws IOException {

        try (FileChannel channel = FileChannel.open(
                file.toPath(),
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE_NEW)) {

            final ByteBuffer offsets = ByteBuffer.allocate(rows * Long.BYTES);
            while (cellIterator.hasNext()) {

                offsets.putLong(channel.position());
                final Cell cell = cellIterator.next();
                final ByteBuffer key = cell.getKey();
                final Value value = cell.getValue();
                channel.write(ByteBuffer.allocate(Integer.BYTES).putInt(key.remaining()).rewind());
                channel.write(key.rewind());

                if (value.isTombstone()) {
                    channel.write(ByteBuffer
                            .allocate(Long.BYTES)
                            .putLong(-1 * value.getTimestamp())
                            .rewind());
                } else {
                    channel.write(ByteBuffer
                            .allocate(Long.BYTES)
                            .putLong(value.getTimestamp())
                            .rewind());
                    channel.write(ByteBuffer
                            .allocate(Integer.BYTES)
                            .putInt(value.getData().remaining())
                            .rewind());
                    channel.write(value
                            .getData()
                            .rewind());
                }
            }

            channel.write(offsets.rewind());
            channel.write(ByteBuffer.allocate(Integer.BYTES).putInt(rows).rewind());
        }
    }

    private int binarySearch(@NotNull final ByteBuffer from) throws IOException{
        assert rows > 0;

        int left = 0;
        int right = rows - 1;
        while (left <= right) {
            final int mid = (right + left) / 2;
            final int cmp = from.compareTo(keyAt(mid));
            if (cmp < 0) {
                right = mid - 1;
            } else if (cmp > 0) {
                left = mid + 1;
            } else {
                return mid;
            }
        }
        return left;
    }

    private long getOffset(final int row) throws IOException {
        final ByteBuffer offset = ByteBuffer.allocate(Long.BYTES);
        channel.read(offset, this.fileSize - Integer.BYTES - Long.BYTES * (rows - row));
        return offset.rewind().getLong();
    }

    @NotNull
    private ByteBuffer keyAt(final int row) throws IOException {
        assert 0 <= row && row <= rows;
        final long offset = getOffset(row);

        final ByteBuffer keyLengthBuffer = ByteBuffer.allocate(Integer.BYTES);
        channel.read(keyLengthBuffer, offset);

        final ByteBuffer keyBuffer = ByteBuffer.allocate(keyLengthBuffer.rewind().getInt());
        channel.read(keyBuffer, offset + Integer.BYTES);
        return keyBuffer.rewind();
    }

    @NotNull
    private Value valueAt(final int row) throws IOException {
        assert 0 <= row && row <= rows;
        final long offset = getOffset(row);

        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        channel.read(buffer, offset);
        final int keyLength = buffer.rewind().getInt();

        buffer = ByteBuffer.allocate(Long.BYTES);
        channel.read(buffer, offset + Integer.BYTES + keyLength);
        final long timestamp = buffer.rewind().getLong();

        buffer = ByteBuffer.allocate(Integer.BYTES);
        channel.read(buffer, offset + Integer.BYTES + keyLength + Long.BYTES);
        final int valueLength = buffer.rewind().getInt();

        buffer = ByteBuffer.allocate(valueLength);
        channel.read(buffer, offset + Integer.BYTES + keyLength + Long.BYTES + Integer.BYTES);

        return timestamp >= 0 ? new Value(timestamp, buffer.rewind()) : new Value(-timestamp);
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull ByteBuffer from) throws IOException {
        return new Iterator<>() {
            private int next = binarySearch(from);

            @Override
            public boolean hasNext() {
                return next < rows;
            }

            @Override
            public Cell next() {
                try {
                    return new Cell(keyAt(next), valueAt(next++));
                } catch (IOException e) {
                    return null;
                }
            }
        };
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) {
        throw new UnsupportedOperationException("Immutable!");
    }

    @Override
    public void remove(@NotNull ByteBuffer key) {
        throw new UnsupportedOperationException("Immutable!");
    }

    @Override
    public long sizeInBytes() {
        return fileSize;
    }

    @Override
    public int size() {
        return rows;
    }

    @Override
    public void close() throws IOException{
        channel.close();
    }
}
