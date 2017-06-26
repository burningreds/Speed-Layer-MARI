/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cl.uchile.tarea3.bolts;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Iterator;
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
 * Obtains itemid name from every product in every boleta
 * @author Paula
 *
 */
public class GetProductsBolt extends BaseBasicBolt {

    private static final Logger LOG = LoggerFactory.getLogger(GetProductsBolt.class);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        try {
            //mapeamos el objeto
            JsonNode object = new ObjectMapper().readTree(tuple.getString(0));
                        
            Iterator<JsonNode> products = object.get("productos").elements(); 
            
            //Para cada producto en la boleta
            while (products.hasNext()) {
                JsonNode product = products.next();
                try {
                	int itemId = product.get("itemId").asInt();
                	String name = product.get("name").toString();
                	collector.emit(new Values(itemId, name));
                	//System.out.println(itemId+name);
                } catch (NullPointerException e) {
                	;
                }
            	
            }
        } catch (IOException ex) {
            java.util.logging.Logger.getLogger(GetProductsBolt.class.getName()).log(Level.SEVERE, null, ex);
        } 

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //nombre que tendran los valores
        declarer.declare(new Fields("itemId", "name"));
    }
}
