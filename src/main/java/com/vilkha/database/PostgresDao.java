package com.vilkha.database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;

public class PostgresDao {

    private final ConnectionFactory connectionFactory;

    public PostgresDao(ConnectionFactory connectionFactory) {
        this.connectionFactory = Objects.requireNonNull(connectionFactory);
    }

    public void execute(String sql) throws Exception {
        try (Connection c = connectionFactory.get(); Statement st = c.createStatement()) {
            st.execute(sql);
        }
    }

    public Set<String> fetchColumns(String tableName) throws Exception {
        String sql = """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = ?
                ORDER BY column_name
                """;

        Set<String> cols = new TreeSet<>();
        try (Connection c = connectionFactory.get(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) cols.add(rs.getString(1));
            }
        }
        return cols;
    }

    public void upsertBatch(
            String tableName,
            String keyColumn,
            LinkedHashMap<String, SqlType> schema,
            List<Map<String, Object>> rows
    ) throws Exception {

        List<String> columns = new ArrayList<>(schema.keySet());

        if (!columns.contains(keyColumn)) {
            throw new IllegalArgumentException("columns must contain keyColumn: " + keyColumn);
        }

        String insertColsSql = join(columns, c -> "\"" + c + "\"", ", ");
        String placeholders  = join(columns, c -> "?", ", ");

        List<String> updCols = new ArrayList<>();
        for (String c : columns) {
            if (!c.equals(keyColumn)) updCols.add(c);
        }
        if (updCols.isEmpty()) {
            throw new IllegalStateException("No updatable columns for table " + tableName);
        }

        String updateSql = join(updCols, c -> "\"" + c + "\" = EXCLUDED.\"" + c + "\"", ", ");

        String upsertSql =
                "INSERT INTO \"" + tableName + "\" (" + insertColsSql + ")\n" +
                        "VALUES (" + placeholders + ")\n" +
                        "ON CONFLICT (\"" + keyColumn + "\") DO UPDATE SET " + updateSql;

        try (Connection c = connectionFactory.get(); PreparedStatement ps = c.prepareStatement(upsertSql)) {
            c.setAutoCommit(false);

            for (Map<String, Object> row : rows) {
                Object keyVal = row.get(keyColumn);
                if (keyVal == null || String.valueOf(keyVal).trim().isEmpty()) {
                continue;
                }

                for (int i = 0; i < columns.size(); i++) {
                    String col = columns.get(i);
                    SqlType type = schema.get(col);
                    Object raw = row.get(col);

                    bind(ps, i + 1, type, raw);
                }

                ps.addBatch();
            }

            ps.executeBatch();
            c.commit();
        }
    }

    private static void bind(PreparedStatement ps, int idx, SqlType type, Object raw) throws SQLException {
        if (raw == null) {
            ps.setNull(idx, type.jdbcType());
            return;
        }

        String s = String.valueOf(raw).trim();
        if (s.isEmpty()) {
            ps.setNull(idx, type.jdbcType());
            return;
        }

        switch (type) {
            case BIGINT -> ps.setLong(idx, Long.parseLong(s));
            case INTEGER -> ps.setInt(idx, Integer.parseInt(s));
            case DECIMAL -> ps.setBigDecimal(idx, new java.math.BigDecimal(s.replace(',', '.')));
            case BOOLEAN -> ps.setBoolean(idx, parseBool(s));
            case TEXT, VARCHAR -> ps.setString(idx, s);
            default -> ps.setObject(idx, s); // безопасный запасной вариант
        }
    }

    private static String join(List<String> items, Function<String, String> mapper, String delim) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < items.size(); i++) {
            if (i > 0) sb.append(delim);
            sb.append(mapper.apply(items.get(i)));
        }
        return sb.toString();
    }

    private static boolean parseBool(String s) {
        String v = s.toLowerCase();
        return v.equals("1") || v.equals("true") || v.equals("yes") || v.equals("y");
    }
}
