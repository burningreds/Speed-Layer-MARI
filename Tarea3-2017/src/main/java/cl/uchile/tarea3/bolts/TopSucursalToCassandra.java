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
 * Updates topsucursal table (sucursalid, totalSales, address, updatedate)
 * @author Paula
 *
 */
public class TopSucursalToCassandra extends CassandraBaseBolt {

    private static final Logger LOG = LoggerFactory.getLogger(TopSucursalToCassandra.class);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
    	Timestamp timestamp = new Timestamp(System.currentTimeMillis());
    	session.execute("TRUNCATE topsucursal");
    	List<Rankable> rankings = ((Rankings)tuple.getValue(0)).getRankings();
    	for (Rankable rank : rankings) {
    		int sucursalId = Integer.parseInt(rank.getObject().toString());
    		double total = (double)rank.getCount()/100.0;
    		String addr = null;
    		try {
    			addr = rank.toString().split("\\|")[2].replaceAll("\\[|\\]", "");
    		} catch (Exception e) {
    			addr = "";
    		}
    		System.out.println(addr);
    		Statement statement = QueryBuilder.insertInto("topsucursal")
    				.value("sucursal_id", sucursalId)
    				.value("total_sales", total)
    				.value("address", addr)
    				.value("update_datetime", timestamp);
    		LOG.debug(statement.toString());
    		session.execute(statement);
    	}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

}
