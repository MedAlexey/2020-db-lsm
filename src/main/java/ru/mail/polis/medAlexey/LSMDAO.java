package ru.mail.polis.medAlexey;

import org.slf4j.Logger;
import ru.mail.polis.DAO;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;
import org.slf4j.LoggerFactory;
import org.jetbrains.annotations.NotNull;
import com.google.common.collect.Iterators;

import java.io.File;
import java.util.List;
import java.util.TreeMap;
import java.util.Iterator;
import java.nio.file.Path;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.NavigableMap;
import java.util.stream.Stream;
import java.nio.file.StandardCopyOption;

public class LSMDAO implements DAO {

    private static final String SUFFIX = ".dat";
    private static final String TEMP = ".tmp";

    @NotNull
    private final File storage;
    private final long flushThreshold;

    //Data
    private Table memTable;
    private final NavigableMap<Integer, Table> ssTables;

    //State
    private int generation = 0;

    private static final Logger logger = LoggerFactory.getLogger(LSMDAO.class);

    public LSMDAO(
            @NotNull final File storage,
            final long flushThreshold) throws IOException {

        this.flushThreshold = flushThreshold;
        assert flushThreshold > 0L;
        this.storage = storage;
        this.memTable = new MemTable();
        this.ssTables = new TreeMap<>();
        try(final Stream<Path> files = Files.list(storage.toPath())) {
            files.filter(
                    path -> path.toString().endsWith(SUFFIX)).forEach(
                            f -> {
                                try {
                                    String name = f.getFileName().toString();
                                    final int generation = Integer.parseInt(name.substring(0, name.indexOf(SUFFIX)));
                                    this.generation = Math.max(this.generation, generation);
                                    ssTables.put(generation, new SSTable(f.toFile()));
                                } catch (Exception e) {
                                    logger.info("Exception in LSMDAO constructor.");
                                }
                            });
        }
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull ByteBuffer from) throws IOException {
        final List<Iterator<Cell>> iters = new ArrayList<>(ssTables.size() + 1);
        iters.add(memTable.iterator(from));
        ssTables.descendingMap().values().forEach(t -> {
            try {
                iters.add(t.iterator(from));
            } catch (IOException e) {
                logger.info("IOException in LSMDAO.iterator().");
            }
        });
        final Iterator<Cell> merge = Iterators.mergeSorted(iters, Cell.COMPARATOR);
        final Iterator<Cell> fresh = Iters.collapseEquals(merge, Cell::getKey);
        final Iterator<Cell> alive = Iterators.filter(fresh, e -> !e.getValue().isTombstone());
        return Iterators.transform(alive, e -> Record.of(e.getKey(), e.getValue().getData()));
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) throws IOException {
        memTable.upsert(key, value);
        if (memTable.sizeInBytes() > flushThreshold) {
            flush();
        }
    }

    @Override
    public void remove(@NotNull ByteBuffer key) throws IOException {
        memTable.remove(key);
        if (memTable.sizeInBytes() > flushThreshold) {
            flush();
        }
    }

    private void flush() throws IOException{
        final File file = new File(storage, generation + TEMP);
        SSTable.serialize(
                file,
                memTable.iterator(ByteBuffer.allocate(0)),
                memTable.size()
        );
        final File dst = new File(storage, generation + SUFFIX);
        Files.move(file.toPath(), dst.toPath(), StandardCopyOption.ATOMIC_MOVE);
        memTable = new MemTable();
        ssTables.put(generation, new SSTable(dst));
        generation++;
    }

    @Override
    public void close() throws IOException {
        if (memTable.size() > 0) {
            flush();
        }

        for (final Table ssTable: ssTables.values()) {
            ssTable.close();
        }
    }
}
