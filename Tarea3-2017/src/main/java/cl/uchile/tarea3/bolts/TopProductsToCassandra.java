/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cl.uchile.tarea3.bolts;

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import java.sql.Timestamp;
import java.util.List;

import org.apache.storm.starter.tools.Rankable;
import org.apache.storm.starter.tools.Rankings;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Updates top10 products table (productid, productcount, updatedate)
 * @author Paula
 *
 */
public class TopProductsToCassandra extends CassandraBaseBolt {

    private static final Logger LOG = LoggerFactory.getLogger(TopProductsToCassandra.class);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
    	Timestamp timestamp = new Timestamp(System.currentTimeMillis());
    	session.execute("TRUNCATE top10categories");
    	List<Rankable> rankings = ((Rankings)tuple.getValue(0)).getRankings();
    	for (Rankable rank : rankings) {
    		int itemId = Integer.parseInt(rank.getObject().toString());
    		long count = rank.getCount();
    		String name = null;
    		try {
    			name = rank.toString().split("\\|")[2].replaceAll("\\[|\\]", "");
    		} catch (Exception e) {
    			name = "";
    		}
    		System.out.println(name);
    		Statement statement = QueryBuilder.insertInto("top10products")
    				.value("item_id", itemId)
    				.value("count", count)
    				.value("name", name)
    				.value("update_datetime", timestamp);
    		LOG.debug(statement.toString());
    		session.execute(statement);
    	}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

}
