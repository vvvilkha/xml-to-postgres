package com.vilkha;

import com.vilkha.config.AppConfig;
import com.vilkha.database.ConnectionFactory;
import com.vilkha.database.DbConfig;
import com.vilkha.database.JdbcConnectionFactory;
import com.vilkha.database.PostgresDao;
import com.vilkha.service.XmlToPostgresService;
import com.vilkha.xml.XmlCatalogParser;

import java.util.List;
import java.util.Locale;
import java.util.Scanner;


public final class Main {

    public static void main(String[] args) throws Exception {
        AppConfig cfgFile = AppConfig.load();

        String xmlUrl = cfgFile.get("xml.url", null);
        require(xmlUrl, "xml.url is required");

        String dbUrl  = cfgFile.get("db.url", null);
        String dbUser = cfgFile.get("db.user", null);
        String dbPass = cfgFile.get("db.pass", null);

        DbConfig dbCfg = new DbConfig(dbUrl, dbUser, dbPass);
        ConnectionFactory cf = new JdbcConnectionFactory(dbCfg);
        PostgresDao dao = new PostgresDao(cf);

        XmlCatalogParser parser = new XmlCatalogParser(xmlUrl);
        XmlToPostgresService service = new XmlToPostgresService(parser, dao);

        runInteractive(service);
    }

    private static void runInteractive(XmlToPostgresService service) {
        System.out.println("""
            Interactive mode.
            Commands:
              tables
              ddl <table>
              columns <table>
              update [table]
              isId <table> <column>
              ddlChange <table>
              help
              exit
            """);

        try (Scanner sc = new Scanner(System.in)) {
            while (true) {
                System.out.print("> ");
                if (!sc.hasNextLine()) return;

                String line = sc.nextLine().trim();
                if (line.isEmpty()) continue;

                String[] p = line.split("\\s+");
                String cmd = p[0].toLowerCase(Locale.ROOT);

                try {
                    switch (cmd) {
                        case "exit", "quit" -> { return; }
                        case "help" -> {
                            System.out.println("tables | ddl <table> | columns <table> | update [table] | isId <table> <column> | ddlChange <table> | exit");
                        }
                        case "tables" -> System.out.println(service.getTableNames());

                        case "ddl" -> {
                            if (p.length < 2) {
                                System.out.println("Usage: ddl <table>");
                                break;
                            }
                            String table = requireTable(service, p[1]);
                            System.out.println(service.getTableDDL(table));
                        }

                        case "columns" -> {
                            if (p.length < 2) {
                                System.out.println("Usage: columns <table>");
                                break;
                            }
                            String table = requireTable(service, p[1]);
                            System.out.println(service.getColumnNames(table));
                        }

                        case "update" -> {
                            if (p.length >= 2) {
                                String table = requireTable(service, p[1]);
                                service.update(table);
                                System.out.println("OK: updated " + table);
                            } else {
                                service.update();
                                System.out.println("OK: updated all");
                            }
                        }

                        case "isid" -> {
                            if (p.length < 3) {
                                System.out.println("Usage: isId <table> <column>");
                                break;
                            }
                            String table = requireTable(service, p[1]);
                            String col = requireIdent(p[2]);
                            System.out.println(service.isColumnId(table, col));
                        }

                        case "ddlchange" -> {
                            if (p.length < 2) {
                                System.out.println("Usage: ddlChange <table>");
                                break;
                            }
                            String table = requireTable(service, p[1]);
                            System.out.println(service.getDDLChange(table));
                        }

                        default -> System.out.println("Unknown command: " + cmd + ". Type: help");
                    }
                } catch (Exception e) {
                    System.out.println("ERROR: " + e.getMessage());
                    e.printStackTrace(System.out);
                }
            }
        }
    }

    private static void require(String value, String message) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(message);
    }

    private static String requireTable(XmlToPostgresService service, String table) {
        if (table == null || table.isBlank()) throw new IllegalArgumentException("table is required");
        List<String> allowed = service.getTableNames();
        if (!allowed.contains(table)) {
            throw new IllegalArgumentException("Unknown table: " + table + ". Allowed: " + allowed);
        }
        return table;
    }

    private static String requireIdent(String name) {
        if (name == null || name.isBlank()) throw new IllegalArgumentException("identifier is required");
        if (!name.matches("[a-zA-Z_][a-zA-Z0-9_]*")) {
            throw new IllegalArgumentException("Invalid identifier: " + name);
        }
        return name;
    }
}