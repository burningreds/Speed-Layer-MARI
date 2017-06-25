package cl.uchile.tarea3.bolts;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import java.util.Map;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseBasicBolt;

public abstract class CassandraBaseBolt extends BaseBasicBolt {

    private Cluster cluster;
    protected Session session;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        cluster = Cluster.builder().addContactPoint("localhost").build();
        session = cluster.connect("tarea3");
    }

    @Override
    public void cleanup() {
        session.close();
        cluster.close();
    }
}
