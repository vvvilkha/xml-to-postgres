package com.vilkha.database;

import java.sql.Types;

public enum SqlType {
    BIGINT("bigint", Types.BIGINT),
    INTEGER("integer", Types.INTEGER),
    DECIMAL("numeric", Types.NUMERIC),
    BOOLEAN("boolean", Types.BOOLEAN),
    TEXT("text", Types.VARCHAR),
    VARCHAR("varchar", Types.VARCHAR);

    private final String ddl;
    private final int jdbcType;

    SqlType(String ddl, int jdbcType) {
        this.ddl = ddl;
        this.jdbcType = jdbcType;
    }

    public String ddl() {
        return ddl;
    }

    public int jdbcType() {
        return jdbcType;
    }
}

