/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cl.uchile.tarea3.bolts;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * Inserts new category count into cassandra table (categoryname, categorycount)
 * @author Paula
 *
 */
public class SucursalesFromCassandra extends CassandraBaseBolt {

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        //Consultamos por el registro con la fecha solicitada
        ResultSet results = session.execute("SELECT * FROM sucursal_count");
        for (Row row : results) {
            long sucursalCount = row.getLong("count");
            String sucursalId = row.getString("sucursal_id");
            //Emitimos los valores
            collector.emit(new Values(sucursalId, sucursalCount));
        }        
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //nombre que tendran los valores
        declarer.declare(new Fields("sucursal", "count"));
    }

}
