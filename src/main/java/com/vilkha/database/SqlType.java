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

    SqlType(String ddl, int type) {
        this.ddl = ddl;
    }

    public String ddl() {
        return ddl;
    }

    public int jdbcType() {
        return switch (this) {
            case BIGINT -> Types.BIGINT;
            case INTEGER -> Types.INTEGER;
            case DECIMAL -> Types.NUMERIC;
            case BOOLEAN -> Types.BOOLEAN;
            case TEXT, VARCHAR -> Types.VARCHAR;
        };
    }
}
