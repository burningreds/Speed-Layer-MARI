/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cl.uchile.tarea3;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.logging.Level;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author FelipeEsteban
 */
public class FilterBolt extends BaseBasicBolt {

    private static final Logger LOG = LoggerFactory.getLogger(FilterBolt.class);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        try {
            //mapeamos el objeto
            JsonNode object = new ObjectMapper().readTree(tuple.getString(0));

            //obtenemos rut cliente
            String rut_cliente = object.get("cliente").toString();
            System.out.println(rut_cliente);
            if ("".equals(rut_cliente)) {
                rut_cliente = null;
            }

            //obtenemos fecha como string
            String fecha = object.get("fecha").textValue();

            //Formato que viene
            DateTimeFormatter a = DateTimeFormatter.ofPattern("yyyy/M/d H:m:s");
            LocalDateTime b = LocalDateTime.from(a.parse(fecha));
            
            //Formato que queremos
            DateTimeFormatter c = DateTimeFormatter.ofPattern("yyyy-MM-dd");

            //Emitimos los valores
            collector.emit(new Values(rut_cliente, b.format(c)));

        } catch (IOException ex) {
            java.util.logging.Logger.getLogger(FilterBolt.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //nombre que tendran los valores
        declarer.declare(new Fields("cliente", "date"));
    }
}
