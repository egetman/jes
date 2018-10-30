package io.jes.provider.jdbc;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PostgresDDLTest {

    @Test
    void postgreDDLShouldHandleInvariants() {
        assertThrows(IllegalArgumentException.class, () -> new PostgresDDL("FOO").createStore(Byte.class));
    }

}