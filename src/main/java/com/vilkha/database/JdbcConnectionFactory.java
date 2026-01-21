package com.vilkha.database;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;



public final class JdbcConnectionFactory implements ConnectionFactory {

    private final DbConfig cfg;

    public JdbcConnectionFactory(DbConfig cfg) {
        this.cfg = Objects.requireNonNull(cfg);
    }

    @Override
    public Connection get() throws SQLException {
        return DriverManager.getConnection(cfg.url(), cfg.user(), cfg.pass());
    }
}
