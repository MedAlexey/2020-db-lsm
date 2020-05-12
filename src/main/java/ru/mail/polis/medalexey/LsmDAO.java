package ru.mail.polis.medalexey;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.DAO;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.stream.Stream;

public class LsmDAO implements DAO {

    private static final String SUFFIX = ".dat";
    private static final String TEMP = ".tmp";

    @NotNull
    private final File storage;
    private final long flushThreshold;

    //Data
    private Table memTable;
    private final NavigableMap<Integer, Table> ssTables;

    //State
    private int generation;

    private static final Logger logger = LoggerFactory.getLogger(LsmDAO.class);

    /**
     * @param storage storage path to sstable directory
     * @param flushThreshold size on bytes that need to flush mem table
     * @throws IOException incorrect base
     */
    public LsmDAO(
            @NotNull final File storage,
            final long flushThreshold) throws IOException {

        this.generation = 0;
        this.flushThreshold = flushThreshold;
        assert flushThreshold > 0L;
        this.storage = storage;
        this.memTable = new MemTable();
        this.ssTables = new TreeMap<>();
        try (Stream<Path> files = Files.list(storage.toPath())) {
            files.filter(
                    path -> path.toString().endsWith(SUFFIX)).forEach(
                            f -> {
                                try {
                                    final String name = f.getFileName().toString();
                                    final int fileGeneration =
                                            Integer.parseInt(name.substring(0, name.indexOf(SUFFIX)));
                                    this.generation = Math.max(this.generation, fileGeneration);
                                    ssTables.put(fileGeneration, new SSTable(f.toFile()));
                                } catch (IOException e) {
                                    logger.info("IOException in LSMDAO constructor.");
                                } catch (NumberFormatException e) {
                                    logger.info("NumberFormatException in LSMDAO constructor.");
                                }
                            });
        }
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
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
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        memTable.upsert(key, value);
        if (memTable.sizeInBytes() > flushThreshold) {
            flush();
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memTable.remove(key);
        if (memTable.sizeInBytes() > flushThreshold) {
            flush();
        }
    }

    private void flush() throws IOException {
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
