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
 * Updates top10 categories table (categoryname, categorycount)
 * @author Paula
 *
 */
public class TopCategoriesToCassandra extends CassandraBaseBolt {

    private static final Logger LOG = LoggerFactory.getLogger(TopCategoriesToCassandra.class);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
    	Timestamp timestamp = new Timestamp(System.currentTimeMillis());
    	session.execute("TRUNCATE top10categories");
    	List<Rankable> rankings = ((Rankings)tuple.getValue(0)).getRankings();
    	for (Rankable rank : rankings) {
    		String categoryName = rank.getObject().toString();
    		long count = rank.getCount();
    		System.out.println("Categoría: " + categoryName + count);
    		Statement statement = QueryBuilder.insertInto("top10categories")
    				.value("category", categoryName)
    				.value("count", count)
    				.value("update_datetime", timestamp);
    		LOG.debug(statement.toString());
    		session.execute(statement);
    	}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

}
