package io.jes.provider.jdbc;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PostgresDDLTest {

    // todo: make ParameterizedTest with some common classes for happy & sad paths
    @Test
    void postgreDDLShouldHandleInvariants() {
        assertThrows(IllegalArgumentException.class, () -> new PostgresDDL("FOO").createStore(Byte.class));
    }

}