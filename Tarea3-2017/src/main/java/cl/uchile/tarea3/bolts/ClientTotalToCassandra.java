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
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Inserts new category count into cassandra table (categoryname, categorycount)
 * @author Paula
 *
 */
public class ClientTotalToCassandra extends CassandraBaseBolt {

    private static final Logger LOG = LoggerFactory.getLogger(ClientTotalToCassandra.class);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String clientId = tuple.getStringByField("clientId");
        double totalBoleta = tuple.getDoubleByField("total"); 
        System.out.println("Cliente: " + clientId + ": " + totalBoleta);
        
        double totalSum = totalBoleta;
        long count = 1;
        
        //Consultamos por el registro con id cliente
        ResultSet results = session.execute("SELECT * FROM client_total WHERE client ='" + clientId + "'");
        for (Row row : results) {
            totalSum += row.getDouble("total");
            count += row.getLong("count");
        }

        Statement statement = QueryBuilder.insertInto("client_total")
        		.value("client", clientId)
        		.value("total", totalSum)
        		.value("count", count)
        		.value("update_date", LocalDate.fromMillisSinceEpoch(System.currentTimeMillis()));
        LOG.debug(statement.toString());
        session.execute(statement);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

}
