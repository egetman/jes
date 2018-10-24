package io.jes.common;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import javax.annotation.Nonnull;
import javax.persistence.SharedCacheMode;
import javax.persistence.ValidationMode;
import javax.persistence.spi.ClassTransformer;
import javax.persistence.spi.PersistenceUnitInfo;
import javax.persistence.spi.PersistenceUnitTransactionType;
import javax.sql.DataSource;

import org.hibernate.jpa.HibernatePersistenceProvider;

import io.jes.provider.jpa.StoreEntryFactory;

public class JesUnitInfo implements PersistenceUnitInfo {

    private final DataSource dataSource;
    private final String targetEntityName;

    JesUnitInfo(@Nonnull DataSource dataSource, @Nonnull Class<?> serializationType) {
        this.dataSource = Objects.requireNonNull(dataSource);
        this.targetEntityName = StoreEntryFactory.entryTypeOf(serializationType).getName();
    }

    @Override
    public String getPersistenceUnitName() {
        return "jes";
    }

    @Override
    public String getPersistenceProviderClassName() {
        return HibernatePersistenceProvider.class.getName();
    }

    @Override
    public PersistenceUnitTransactionType getTransactionType() {
        return PersistenceUnitTransactionType.RESOURCE_LOCAL;
    }

    @Override
    public DataSource getJtaDataSource() {
        return dataSource;
    }

    @Override
    public DataSource getNonJtaDataSource() {
        return null;
    }

    @Override
    public List<String> getMappingFileNames() {
        return Collections.emptyList();
    }

    @Override
    public List<URL> getJarFileUrls() {
        try {
            return Collections.list(this.getClass().getClassLoader().getResources(""));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public URL getPersistenceUnitRootUrl() {
        return null;
    }

    @Override
    public List<String> getManagedClassNames() {
        return Collections.singletonList(targetEntityName);
    }

    @Override
    public boolean excludeUnlistedClasses() {
        return true;
    }

    @Override
    public SharedCacheMode getSharedCacheMode() {
        return null;
    }

    @Override
    public ValidationMode getValidationMode() {
        return ValidationMode.AUTO;
    }

    @Override
    public Properties getProperties() {
        final Properties properties = new Properties();
        properties.setProperty(org.hibernate.cfg.AvailableSettings.SCANNER_DISCOVERY, "");
        return properties;
    }

    @Override
    public String getPersistenceXMLSchemaVersion() {
        return null;
    }

    @Override
    public ClassLoader getClassLoader() {
        return null;
    }

    @Override
    public void addTransformer(ClassTransformer transformer) {

    }

    @Override
    public ClassLoader getNewTempClassLoader() {
        return null;
    }

}
