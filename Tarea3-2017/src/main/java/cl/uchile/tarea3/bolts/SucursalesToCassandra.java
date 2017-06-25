/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cl.uchile.tarea3.bolts;

import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Inserts new sucursal count into cassandra table (sucursalId, count)
 * The idea is to update the previous data with the new count (i think)
 * @author Paula
 *
 */
public class SucursalesToCassandra extends CassandraBaseBolt {

    private static final Logger LOG = LoggerFactory.getLogger(SucursalesToCassandra.class);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        long sucursalCount = 0;
        String sucursalId = tuple.getStringByField("sucursal");
        System.out.println("Sucursal: " + sucursalId);
        //Consultamos por el registro con la fecha solicitada
        ResultSet results = session.execute("SELECT * FROM sucursal_count WHERE sucursal_id ='" + sucursalId + "'");
        for (Row row : results) {
            sucursalCount = row.getLong("count") + 1;
        }

        Statement statement = QueryBuilder.insertInto("category_count")
        		.value("sucursal_id", sucursalId)
        		.value("count", sucursalCount)
        		.value("update_date", LocalDate.fromMillisSinceEpoch(System.currentTimeMillis()));
        LOG.debug(statement.toString());
        session.execute(statement);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	;
    }

}
