/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cl.uchile.tarea3;

import static cl.uchile.tarea3.Cassandra.getTimeFromUUID;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import cl.uchile.tarea3.bolts.CassandraBaseBolt;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;
import java.util.logging.Level;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author FelipeEsteban
 */
public class ProbabilidadToCassandra extends CassandraBaseBolt {

    private static final Logger LOG = LoggerFactory.getLogger(ProbabilidadToCassandra.class);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        long entrega_rut = 0, total_rut = 0;
        System.out.println("Cliente: " +tuple.getStringByField("cliente")+ tuple.getStringByField("date"));

        //Consultamos por el registro con la fecha solicitada
        ResultSet results = session.execute("SELECT * FROM probabilidad WHERE date='" + tuple.getStringByField("date") + "'");
        for (Row row : results) {
            entrega_rut = row.getLong("entrega_rut");
            total_rut = row.getLong("total_rut");
        }

        String[] a = tuple.getStringByField("date").split("-");

        if ("null".equals(tuple.getStringByField("cliente"))) {
            //Sumamos solo al total si el usuario es null
            Statement statement = QueryBuilder.insertInto("probabilidad")
                    .value("date", LocalDate.fromYearMonthDay(Integer.parseInt(a[0]), Integer.parseInt(a[1]), Integer.parseInt(a[2])))
                    .value("entrega_rut", entrega_rut)
                    .value("total_rut", total_rut + 1);
            LOG.debug(statement.toString());
            session.execute(statement);
        } else {
            //Si el usuario es distinto de null, sumamos en ambos lados
            Statement statement = QueryBuilder.insertInto("probabilidad")
                    .value("date", LocalDate.fromYearMonthDay(Integer.parseInt(a[0]), Integer.parseInt(a[1]), Integer.parseInt(a[2])))
                    .value("entrega_rut", entrega_rut + 1)
                    .value("total_rut", total_rut + 1);
            LOG.debug(statement.toString());
            session.execute(statement);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

}
