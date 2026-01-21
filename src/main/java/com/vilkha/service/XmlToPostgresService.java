package com.vilkha.service;

import com.vilkha.database.SqlType;
import com.vilkha.database.PostgresDao;
import com.vilkha.xml.XmlCatalogParser;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

public final class XmlToPostgresService {

    private final XmlCatalogParser parser;
    private final PostgresDao dao;

    public XmlToPostgresService(XmlCatalogParser parser, PostgresDao dao) {
        this.parser = Objects.requireNonNull(parser);
        this.dao = Objects.requireNonNull(dao);
    }

    public List<String> getTableNames() {
        return new ArrayList<>(parser.getTableNames());
    }

    public String getTableDDL(String tableName) throws Exception {
        String idCol = idColumn(tableName);
        LinkedHashMap<String, SqlType> schema = parser.inferSchema(tableName, idCol);

        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE IF NOT EXISTS \"").append(tableName).append("\" (\n");

        int i = 0;
        for (Map.Entry<String, SqlType> e : schema.entrySet()) {
            if (i++ > 0) sb.append(",\n");
            sb.append("  \"").append(e.getKey()).append("\" ").append(e.getValue().ddl());
        }

        if (idCol != null && schema.containsKey(idCol)) {
            sb.append(",\n  CONSTRAINT \"pk_").append(tableName)
                    .append("\" PRIMARY KEY (\"").append(idCol).append("\")");
        }

        sb.append("\n);\n");
        return sb.toString();
    }

    public void update() throws Exception {
        update("currency");
        update("categories");
        update("offers");
    }

    public void update(String tableName) throws Exception {
        String idCol = idColumn(tableName);

        LinkedHashMap<String, SqlType> xmlSchema = parser.inferSchema(tableName, idCol);

        dao.execute(getTableDDL(tableName));

        ensureStructureNotChanged(tableName, xmlSchema);

        List<Map<String, Object>> rows = parser.readRows(tableName);
        List<String> columns = new ArrayList<>(xmlSchema.keySet());
        dao.upsertBatch(tableName, idCol, xmlSchema, rows);
    }

    public ArrayList<String> getColumnNames(String tableName) throws Exception {
        String idCol = idColumn(tableName);
        LinkedHashMap<String, SqlType> schema = parser.inferSchema(tableName, idCol);
        return new ArrayList<>(schema.keySet());
    }

    public boolean isColumnId(String tableName, String columnName) throws Exception {
        List<Map<String, Object>> rows = parser.readRows(tableName);
        Set<Object> seen = new HashSet<>();
        for (Map<String, Object> row : rows) {
            Object v = row.get(columnName);
            if (v == null) return false;
            if (!seen.add(v)) return false;
        }
        return true;
    }

    public String getDDLChange(String tableName) throws Exception {
        String idCol = idColumn(tableName);
        LinkedHashMap<String, SqlType> xmlSchema = parser.inferSchema(tableName, idCol);
        Set<String> dbCols = dao.fetchColumns(tableName);

        List<String> missing = new ArrayList<>();
        for (String col : xmlSchema.keySet()) {
            if (!dbCols.contains(col)) missing.add(col);
        }

        if (missing.isEmpty()) return "-- no changes\n";

        StringBuilder sb = new StringBuilder();
        for (String col : missing) {
            SqlType t = xmlSchema.get(col);
            sb.append("ALTER TABLE \"").append(tableName)
                    .append("\" ADD COLUMN \"").append(col)
                    .append("\" ").append(t.ddl()).append(";\n");
        }
        return sb.toString();
    }

    private void ensureStructureNotChanged(String tableName, LinkedHashMap<String, SqlType> xmlSchema)
            throws Exception {

        Set<String> dbCols = dao.fetchColumns(tableName);
        Set<String> xmlCols = new TreeSet<>(xmlSchema.keySet());

        if (!dbCols.equals(xmlCols)) {
            Set<String> onlyDb = new TreeSet<>(dbCols);
            onlyDb.removeAll(xmlCols);

            Set<String> onlyXml = new TreeSet<>(xmlCols);
            onlyXml.removeAll(dbCols);

            throw new IllegalStateException(
                    "Structure changed for table '" + tableName + "'. " +
                            "Only in DB: " + onlyDb + "; Only in XML: " + onlyXml
            );
        }
    }

    private String idColumn(String tableName) {
        if ("offers".equals(tableName)) return "vendorCode";
        return "id";
    }
}

