package com.vilkha.xml;

import com.vilkha.database.SqlType;
import groovy.xml.XmlSlurper;
import groovy.xml.XmlUtil;
import groovy.xml.slurpersupport.GPathResult;
import org.codehaus.groovy.runtime.InvokerHelper;

import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Pattern;

public class XmlCatalogParser {

    private final String xmlUrl;
    private GPathResult cachedDoc;

    private final Map<String, List<Map<String, Object>>> cachedRows = new HashMap<>();
    private final Map<String, LinkedHashMap<String, SqlType>> cachedSchema = new HashMap<>();

    private static final Pattern INT = Pattern.compile("-?\\d+");
    private static final Pattern NUM = Pattern.compile("-?\\d+(\\.\\d+)?");

    public XmlCatalogParser(String xmlUrl) {
        this.xmlUrl = Objects.requireNonNull(xmlUrl);
    }

    public List<String> getTableNames() {
        return List.of("currency", "categories", "offers");
    }

    public List<Map<String, Object>> readRows(String tableName) throws Exception {
        if (cachedRows.containsKey(tableName)) {
            return cachedRows.get(tableName);
        }

        GPathResult doc = xmlDoc();
        GPathResult shop = (GPathResult) doc.getProperty("shop");

        List<Map<String, Object>> rows = new ArrayList<>();

        switch (tableName) {
            case "currency" -> {
                GPathResult currencies = (GPathResult) shop.getProperty("currencies");
                Iterable<?> currencyNodes = (Iterable<?>) currencies.getProperty("currency");
                for (Object node : currencyNodes) rows.add(nodeToRow((GPathResult) node));
            }
            case "categories" -> {
                GPathResult categories = (GPathResult) shop.getProperty("categories");
                Iterable<?> categoryNodes = (Iterable<?>) categories.getProperty("category");
                for (Object node : categoryNodes) {
                    GPathResult cat = (GPathResult) node;
                    Map<String, Object> row = nodeToRow(cat);

                    String text = safeTrim(cat.text());
                    if (!text.isEmpty() && !row.containsKey("value")) {
                        row.put("value", text);
                    }
                    rows.add(row);
                }
            }
            case "offers" -> {
                GPathResult offers = (GPathResult) shop.getProperty("offers");
                Iterable<?> offerNodes = (Iterable<?>) offers.getProperty("offer");
                for (Object node : offerNodes) rows.add(nodeToRow((GPathResult) node));
            }
            default -> throw new IllegalArgumentException("Unknown tableName: " + tableName);
        }

        if (rows.isEmpty()) {
            throw new IllegalStateException("No rows found for table '" + tableName + "' in XML");
        }

        cachedRows.put(tableName, rows);
        return rows;
    }

    public LinkedHashMap<String, SqlType> inferSchema(String tableName, String idColumn) throws Exception {
        if (cachedSchema.containsKey(tableName)){
            return cachedSchema.get(tableName);
        }

        List<Map<String, Object>> rows = readRows(tableName);

        LinkedHashSet<String> cols = new LinkedHashSet<>();
        for (Map<String, Object> row : rows) cols.addAll(row.keySet());
        if (idColumn != null) cols.add(idColumn);

        LinkedHashMap<String, SqlType> schema = new LinkedHashMap<>();
        for (String c : cols) {
            List<String> values = new ArrayList<>();
            for (Map<String, Object> row : rows) {
                Object v = row.get(c);
                if (v != null) {
                    String s = String.valueOf(v).trim();
                    if (!s.isEmpty()) values.add(s);
                }
            }
            schema.put(c, guessType(values));
        }

        cachedSchema.put(tableName, schema);
        return schema;
    }

    private GPathResult xmlDoc() throws Exception {
        if (cachedDoc != null) return cachedDoc;

        URL url = URI.create(xmlUrl).toURL();

        String xml;
        try (InputStream is = url.openStream()) {
            xml = new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }

        xml = xml.replaceFirst("(?is)<!DOCTYPE.*?>", "");

        XmlSlurper slurper = new XmlSlurper(false, false);
        cachedDoc = (GPathResult) slurper.parseText(xml);

        return cachedDoc;
    }

    private Map<String, Object> nodeToRow(GPathResult node) {
        Map<String, Object> row = new LinkedHashMap<>();
        Object attrsObj = InvokerHelper.invokeMethod(node, "attributes", new Object[0]);
        if (attrsObj instanceof Map<?, ?> attrs) {
            for (Map.Entry<?, ?> e : attrs.entrySet()) {
                String col = String.valueOf(e.getKey());
                Object value = e.getValue();
                row.put(col, value == null ? null : String.valueOf(value));
            }
        }

        Object childrenObj = InvokerHelper.invokeMethod(node, "children", new Object[0]);
        if (childrenObj instanceof Iterable<?> children) {
            for (Object chObj : children) {
                if (!(chObj instanceof GPathResult ch)) continue;

                Object nameObj = InvokerHelper.invokeMethod(ch, "name", new Object[0]);
                if (nameObj == null) continue;
                String name = String.valueOf(nameObj);

                Object chChildrenObj = InvokerHelper.invokeMethod(ch, "children", new Object[0]);
                int childCount = sizeOfIterable(chChildrenObj instanceof Iterable<?> it ? it : List.of());

                if (childCount == 0) {
                    Object textObj = InvokerHelper.invokeMethod(ch, "text", new Object[0]);
                    String text = textObj == null ? "" : String.valueOf(textObj).trim();
                    row.put(name, text.isEmpty() ? null : text);
                } else {
                    String xml = XmlUtil.serialize(ch).trim();
                    row.put(name, xml);
                }
            }
        }

        return row;
    }

    private static int sizeOfIterable(Iterable<?> it) {
        int n = 0;
        for (Object ignored : it) n++;
        return n;
    }

    private SqlType guessType(List<String> values) {
        if (values == null || values.isEmpty()) return SqlType.TEXT;

        boolean allInt = true;
        boolean allNum = true;

        for (String v : values) {
            if (!INT.matcher(v).matches()) allInt = false;
            if (!NUM.matcher(v).matches()) allNum = false;
            if (!allInt && !allNum) break;
        }

        if (allInt) return SqlType.BIGINT;
        if (allNum) return SqlType.BIGINT;
        return SqlType.TEXT;
    }

    private static String safeTrim(String s) {
        return s == null ? "" : s.trim();
    }
}

