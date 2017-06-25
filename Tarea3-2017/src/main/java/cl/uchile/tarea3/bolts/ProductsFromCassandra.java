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
 * Obtains updated product count from cassandra table (itemId, count)
 * @author Paula
 *
 */
public class ProductsFromCassandra extends CassandraBaseBolt {

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        ResultSet results = session.execute("SELECT * FROM product_count");
        for (Row row : results) {
            long itemCount = row.getLong("count");
            String itemName = row.getString("item_id");
            //Emitimos los valores
            collector.emit(new Values(itemName, itemCount));
        }        
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //nombre que tendran los valores
        declarer.declare(new Fields("item", "count"));
    }

}
