package com.vilkha.database;

import java.util.Objects;

public final class DbConfig {

    private final String url;
    private final String user;
    private final String pass;

    public DbConfig(String url, String user, String pass) {
        this.url = Objects.requireNonNull(url);
        this.user = Objects.requireNonNull(user);
        this.pass = Objects.requireNonNull(pass);
    }

    public String url() {
        return url;
    }

    public String user() {
        return user;
    }

    public String pass() {
        return pass;
    }
}
