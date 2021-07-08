package com.aurora.table;

/**
 * @author lj.michale
 * @description
 * @date 2021-07-08
 */

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.CatalogFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_TYPE;

/**
 * Factory for {@link KuduCatalog}.
 */
@Internal
public class KuduCatalogFactory implements CatalogFactory {

    private static final Logger LOG = LoggerFactory.getLogger(KuduCatalogFactory.class);

    @Override
    public Map<String, String> requiredContext() {
        Map<String, String> context = new HashMap<>();
        context.put(CATALOG_TYPE, KuduTableFactory.KUDU);
        // backwards compatibility
        context.put(CATALOG_PROPERTY_VERSION, "1");
        return context;
    }

    @Override
    public List<String> supportedProperties() {
        List<String> properties = new ArrayList<>();

        properties.add(KuduTableFactory.KUDU_MASTERS);

        return properties;
    }

    @Override
    public Catalog createCatalog(String name, Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = getValidatedProperties(properties);
        return new KuduCatalog(name,
                descriptorProperties.getString(KuduTableFactory.KUDU_MASTERS));
    }

    private DescriptorProperties getValidatedProperties(Map<String, String> properties) {
        final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
        descriptorProperties.putProperties(properties);
        descriptorProperties.validateString(KuduTableFactory.KUDU_MASTERS, false);
        return descriptorProperties;
    }

}