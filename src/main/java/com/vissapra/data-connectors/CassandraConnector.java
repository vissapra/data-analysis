package com.vissapra.clients.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Cassandra connector that creates a Session to a given keyspace
 * 
 */
class CassandraConnector {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraConnector.class);
    private static Cluster cluster;
    private static Configuration configuration;

    public static class Builder {

        static CassandraConnector buildWithConfig(Configuration config) {
            configuration = checkNotNull(config, "config object can't be null");
            String[] hosts = checkNotNull(configuration.getArray(AppSettings.CASSANDRA_CONTACT_POINTS),
                    "Host can not be null or empty");

            cluster = Cluster.builder().addContactPoints(hosts).withPort(config.getAsInt(AppSettings.CASSANDRA_PORT)).build();
            return new CassandraConnector(cluster);
        }
    }

    private CassandraConnector(Cluster cluster) {
        this.cluster = checkNotNull(cluster, "cluster can not be null");
        printMetadata(cluster);
    }

    public void shutdown() {
        cluster.close();
    }

    Session session(String keyspace) {

        return cluster.connect(keyspace);
    }

    private void printMetadata(Cluster cluster) {
        Metadata metadata = cluster.getMetadata();
        StringBuilder message = new StringBuilder(String.format("Cluster:%s\n", metadata.getClusterName()));
        for (Host host : metadata.getAllHosts()) {
            message.append(String.format("Datacenter: %s; Host: %s; Rack: %s\n",
                    host.getDatacenter(), host.getAddress(), host.getRack()));
        }
        message.append(String.format("Partitioner:%s", metadata.getPartitioner()));
        LOG.info("Cluster Details \\n {}", message);
    }

}
