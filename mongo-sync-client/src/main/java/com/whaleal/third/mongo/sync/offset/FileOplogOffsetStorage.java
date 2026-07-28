package com.whaleal.third.mongo.sync.offset;

import com.whaleal.third.mongo.source.model.OplogOffset;
import com.whaleal.third.mongo.source.spi.OplogOffsetStorage;
import org.bson.BsonTimestamp;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 文件持久化 OplogOffset，内容为 {@code t:i}（秒:inc）。
 */
public final class FileOplogOffsetStorage implements OplogOffsetStorage {

    private final Path file;
    private final ReentrantLock lock = new ReentrantLock();

    public FileOplogOffsetStorage(Path file) {
        this.file = file;
    }

    @Override
    public OplogOffset load() {
        lock.lock();
        try {
            if (!Files.isRegularFile(file)) {
                return OplogOffset.empty();
            }
            String text = new String(Files.readAllBytes(file), StandardCharsets.UTF_8).trim();
            if (text.isEmpty()) {
                return OplogOffset.empty();
            }
            int colon = text.indexOf(':');
            if (colon <= 0) {
                throw new IllegalStateException("invalid oplog offset file content: " + text);
            }
            int t = Integer.parseInt(text.substring(0, colon).trim());
            int i = Integer.parseInt(text.substring(colon + 1).trim());
            return OplogOffset.of(new BsonTimestamp(t, i));
        } catch (Exception e) {
            throw new IllegalStateException("load oplog offset failed: " + file, e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void save(OplogOffset offset) {
        lock.lock();
        try {
            Path parent = file.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            String text;
            if (offset == null || offset.isEmpty() || offset.getTimestamp() == null) {
                text = "";
            } else {
                BsonTimestamp ts = offset.getTimestamp();
                text = ts.getTime() + ":" + ts.getInc();
            }
            Path tmp = file.resolveSibling(file.getFileName().toString() + ".tmp");
            Files.write(tmp, text.getBytes(StandardCharsets.UTF_8));
            try {
                Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException moveEx) {
                Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new IllegalStateException("save oplog offset failed: " + file, e);
        } finally {
            lock.unlock();
        }
    }
}
