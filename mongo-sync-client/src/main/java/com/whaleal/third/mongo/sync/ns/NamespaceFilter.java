package com.whaleal.third.mongo.sync.ns;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * 库表白/黑名单过滤（对齐 MongoShake {@code filter.namespace.white/black}）。
 * <p>
 * 规则条目可为：
 * <ul>
 *   <li>{@code db} — 整库</li>
 *   <li>{@code db.collection} — 单集合</li>
 * </ul>
 * 白名单与黑名单不可同时非空（与 Shake 一致：优先使用白名单；二者都空表示「未启用过滤」由调用方决定）。
 */
public final class NamespaceFilter {

    private final Set<String> white;
    private final Set<String> black;

    private NamespaceFilter(Set<String> white, Set<String> black) {
        this.white = white;
        this.black = black;
    }

    public static NamespaceFilter empty() {
        return new NamespaceFilter(Collections.<String>emptySet(), Collections.<String>emptySet());
    }

    /**
     * @param whiteSemicolon 分号分隔，如 {@code db1;db2.coll1}
     * @param blackSemicolon 分号分隔
     */
    public static NamespaceFilter of(String whiteSemicolon, String blackSemicolon) {
        Set<String> white = parse(whiteSemicolon);
        Set<String> black = parse(blackSemicolon);
        if (!white.isEmpty() && !black.isEmpty()) {
            throw new IllegalArgumentException(
                    "namespace white and black list cannot both be set (align MongoShake)");
        }
        return new NamespaceFilter(white, black);
    }

    public boolean isEmpty() {
        return white.isEmpty() && black.isEmpty();
    }

    public boolean hasWhite() {
        return !white.isEmpty();
    }

    public boolean hasBlack() {
        return !black.isEmpty();
    }

    public Set<String> white() {
        return white;
    }

    public Set<String> black() {
        return black;
    }

    /** 系统库默认跳过。 */
    public static boolean isSystemDatabase(String db) {
        if (db == null) {
            return true;
        }
        String d = db.toLowerCase(Locale.ROOT);
        return "admin".equals(d) || "local".equals(d) || "config".equals(d);
    }

    public static boolean isSystemCollection(String collection) {
        return collection != null && collection.startsWith("system.");
    }

    /**
     * @param database   库名
     * @param collection 集合名
     * @return true 表示应同步
     */
    public boolean accept(String database, String collection) {
        if (isSystemDatabase(database) || isSystemCollection(collection)) {
            return false;
        }
        String ns = database + "." + collection;
        if (!white.isEmpty()) {
            return matchList(white, database, ns);
        }
        if (!black.isEmpty()) {
            return !matchList(black, database, ns);
        }
        // 无过滤规则：调用方若用于「整集群」应显式配白名单；此处默认放行非系统库表
        return true;
    }

    private static boolean matchList(Set<String> rules, String database, String ns) {
        if (rules.contains(ns) || rules.contains(database)) {
            return true;
        }
        return false;
    }

    private static Set<String> parse(String semicolonList) {
        if (semicolonList == null || semicolonList.trim().isEmpty()) {
            return Collections.emptySet();
        }
        Set<String> out = new LinkedHashSet<String>();
        String[] parts = semicolonList.split(";");
        for (String part : parts) {
            if (part == null) {
                continue;
            }
            String p = part.trim();
            if (!p.isEmpty()) {
                out.add(p);
            }
        }
        return Collections.unmodifiableSet(out);
    }

    public List<String> describe() {
        List<String> lines = new ArrayList<String>();
        if (!white.isEmpty()) {
            lines.add("white=" + white);
        }
        if (!black.isEmpty()) {
            lines.add("black=" + black);
        }
        return lines;
    }
}
