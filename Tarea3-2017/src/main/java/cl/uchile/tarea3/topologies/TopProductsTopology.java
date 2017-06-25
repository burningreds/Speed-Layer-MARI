/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cl.uchile.tarea3.topologies;

import java.util.Properties;
import org.apache.storm.LocalCluster;
import org.apache.storm.Config;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.starter.bolt.IntermediateRankingsBolt;
import org.apache.storm.starter.bolt.TotalRankingsBolt;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.bolt.KafkaBolt;

import cl.uchile.tarea3.bolts.GetProductsBolt;
import cl.uchile.tarea3.bolts.ProductsFromCassandra;
import cl.uchile.tarea3.bolts.ProductsToCassandra;
import cl.uchile.tarea3.bolts.TopCategoriesToCassandra;

/**
 * Generates product_count (itemId, count, update_date) table
 * and top 10 (itemId, count, update_date) table
 * for querying
 * @author FelipeEsteban
 */
@SuppressWarnings("deprecation")
public class TopProductsTopology {

    public static void main(String[] args) {
        //Configuracion de Storm para que lea la cola Local de Kafka
        String BROKER_LIST = "localhost:9092";
        String KAFKA_TOPIC = "kafkaQ";
        String KAFKA_CONSUMER_GROUP = "storm";
        String ZOOKEEPER_HOST = "localhost:2181";
        String ZOOKEEPER_ROOT = "/mari_spout";
        BrokerHosts hosts = new ZkHosts(ZOOKEEPER_HOST);
        SpoutConfig spoutConfig = new SpoutConfig(
                hosts,
                KAFKA_TOPIC,
                ZOOKEEPER_ROOT,
                KAFKA_CONSUMER_GROUP
        );
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
        Config config = new Config();
        Properties props = new Properties();
        props.put("bootstrap.servers", BROKER_LIST);
        props.put("group.id", KAFKA_CONSUMER_GROUP);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put(KafkaBolt.TOPIC, props);
        //aqui termina la configuración

        //Creamos Topologia
        TopologyBuilder builder = new TopologyBuilder();
        
        //Accedemos al Spout de Kafka definido previamente
        builder.setSpout("KafkaSpout", kafkaSpout);
        
        //
        builder.setBolt("GetProducts", new GetProductsBolt(), 4)
                .shuffleGrouping("KafkaSpout");              
        builder.setBolt("ProductsCount", new ProductsToCassandra(), 4)
        		.fieldsGrouping("GetProducts", new Fields("item"));
        builder.setBolt("GetProductsCount", new ProductsFromCassandra(), 4)
        		.shuffleGrouping("ProductsCount"); 
        builder.setBolt("IntermediateRanker", new IntermediateRankingsBolt(10), 4)
        		.fieldsGrouping("GetProductsCount", new Fields(
                "obj"));
        builder.setBolt("FinalTopCategories", new TotalRankingsBolt(10))
        		.globalGrouping("IntermediateRanker");
        builder.setBolt("TopProductsToCassandra", new TopCategoriesToCassandra(), 4)
        		.shuffleGrouping("FinalTopCategories");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Tarea3", config, builder.createTopology());

    }

}
