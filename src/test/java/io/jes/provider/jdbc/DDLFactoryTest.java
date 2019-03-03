package io.jes.provider.jdbc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class DDLFactoryTest {

    @Test
    void newDDLProducerShouldReturnPostgreSQLDDLProducerOnCorrectValue() {
        Assertions.assertEquals(PostgresDDL.class, DDLFactory.newDDLProducer("PostgreSQL", "FOO").getClass());
    }

    @Test
    void newDDLProducerShouldReturnH2DDLProducerOnCorrectValue() {
        Assertions.assertEquals(H2DDL.class, DDLFactory.newDDLProducer("H2", "FOO").getClass());
    }

    @Test
    void newDDLProducerShouldThrowIllegalArgumentExceptionOnAnyOtherValue() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> DDLFactory.newDDLProducer("Oracle DB", "FOO"));

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> DDLFactory.newDDLProducer("MySQL", "FOO"));

        Assertions.assertThrows(IllegalArgumentException.class,
                () -> DDLFactory.newDDLProducer("DB2", "FOO"));

    }

}