package com.whaleal.third.mongo.sync.offset;

import com.whaleal.third.mongo.source.model.ResumeToken;
import com.whaleal.third.mongo.source.spi.ResumeTokenStorage;
import org.bson.BsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;
import org.bson.json.JsonReader;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 文件持久化 ResumeToken（按路径一份文件，一行 JSON）。
 */
public final class FileResumeTokenStorage implements ResumeTokenStorage {

    private final Path file;
    private final ReentrantLock lock = new ReentrantLock();

    public FileResumeTokenStorage(Path file) {
        this.file = file;
    }

    @Override
    public ResumeToken load() {
        lock.lock();
        try {
            if (!Files.isRegularFile(file)) {
                return ResumeToken.empty();
            }
            byte[] bytes = Files.readAllBytes(file);
            String json = new String(bytes, StandardCharsets.UTF_8).trim();
            if (json.isEmpty() || "{}".equals(json) || "null".equals(json)) {
                return ResumeToken.empty();
            }
            BsonDocument doc = parseJson(json);
            return ResumeToken.fromBson(doc);
        } catch (Exception e) {
            throw new IllegalStateException("load resume token failed: " + file, e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void save(ResumeToken token) {
        lock.lock();
        try {
            Files.createDirectories(file.getParent() == null
                    ? file.toAbsolutePath().getParent()
                    : file.getParent());
            String json = (token == null || token.isEmpty() || token.getToken() == null)
                    ? "{}"
                    : token.getToken().toJson();
            Path tmp = file.resolveSibling(file.getFileName().toString() + ".tmp");
            Files.write(tmp, json.getBytes(StandardCharsets.UTF_8));
            try {
                Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException moveEx) {
                Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new IllegalStateException("save resume token failed: " + file, e);
        } finally {
            lock.unlock();
        }
    }

    private static BsonDocument parseJson(String json) {
        JsonReader reader = new JsonReader(json);
        return new BsonDocumentCodec().decode(reader, DecoderContext.builder().build());
    }
}
