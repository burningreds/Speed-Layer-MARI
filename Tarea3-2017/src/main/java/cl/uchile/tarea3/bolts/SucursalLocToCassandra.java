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
 * Updates sucursal location table (sucursal_id, lng, lat) 
 * sucursales get repeated a lot tho
 * @author Paula
 *
 */
public class SucursalLocToCassandra extends CassandraBaseBolt {

	private static final Logger LOG = LoggerFactory.getLogger(SucursalLocToCassandra.class);

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		int sucursalId = tuple.getIntegerByField("sucursal");
		double lng = tuple.getDoubleByField("lng");
		double lat = tuple.getDoubleByField("lat");

		System.out.println(lng + " " + lat);
		Statement statement = QueryBuilder.insertInto("sucursal_location")
				.value("sucursal_id", sucursalId)
				.value("lng", lng)
				.value("lat", lat);
		LOG.debug(statement.toString());
		session.execute(statement);

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
