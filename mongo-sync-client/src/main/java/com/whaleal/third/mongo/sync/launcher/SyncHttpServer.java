package com.whaleal.third.mongo.sync.launcher;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.whaleal.third.mongo.sync.sdk.MigrationProgress;
import org.bson.Document;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

/**
 * 轻量 HTTP 控制面：对齐 mongosync 常见的 progress / pause / resume / commit 能力。
 */
final class SyncHttpServer implements AutoCloseable {

    private final HttpServer server;

    private SyncHttpServer(HttpServer server) {
        this.server = server;
    }

    static SyncHttpServer start(String host,
                                int port,
                                Callable<MigrationProgress> progressAction,
                                Callable<MigrationProgress> pauseAction,
                                Callable<MigrationProgress> resumeAction,
                                Callable<MigrationProgress> commitAction) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(host, port), 0);
        server.createContext("/api/v1/progress", jsonHandler("GET", progressAction));
        server.createContext("/api/v1/pause", jsonHandler("POST", pauseAction));
        server.createContext("/api/v1/resume", jsonHandler("POST", resumeAction));
        server.createContext("/api/v1/commit", jsonHandler("POST", commitAction));
        server.createContext("/api/v1/canCommit", new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                    writeJson(exchange, 405, new Document("error", "method not allowed"));
                    return;
                }
                try {
                    MigrationProgress progress = progressAction.call();
                    writeJson(exchange, 200, new Document("success", true)
                            .append("canCommit", progress.isCanCommit())
                            .append("state", progress.getState() == null ? null : progress.getState().name())
                            .append("commitReadiness", progress.getCommitReadiness()));
                } catch (Exception e) {
                    writeJson(exchange, 500, errorDoc(e));
                }
            }
        });
        server.setExecutor(Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "mongo-sync-http");
            t.setDaemon(true);
            return t;
        }));
        server.start();
        return new SyncHttpServer(server);
    }

    private static HttpHandler jsonHandler(final String method, final Callable<MigrationProgress> action) {
        return new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                if (!method.equalsIgnoreCase(exchange.getRequestMethod())) {
                    writeJson(exchange, 405, new Document("error", "method not allowed"));
                    return;
                }
                try {
                    MigrationProgress progress = action.call();
                    writeJson(exchange, 200, new Document("success", true)
                            .append("progress", progress == null ? null : progress.toDocument()));
                } catch (Exception e) {
                    writeJson(exchange, 500, errorDoc(e));
                }
            }
        };
    }

    private static Document errorDoc(Exception e) {
        return new Document("success", false)
                .append("error", e == null ? "unknown error" : e.getMessage())
                .append("type", e == null ? null : e.getClass().getSimpleName());
    }

    private static void writeJson(HttpExchange exchange, int status, Document doc) throws IOException {
        byte[] body = doc.toJson().getBytes(StandardCharsets.UTF_8);
        Headers headers = exchange.getResponseHeaders();
        headers.set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(status, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
