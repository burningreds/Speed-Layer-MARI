/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cl.uchile.tarea3;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;

/**
 * Obtains category name from every product bought 
 * in every purchase and the date of the purchase. 
 * @author Paula
 *
 */
public class Test {
	
    public static void main(String[] args) throws JsonProcessingException, IOException {
        JsonNode object = new ObjectMapper().readTree("{\"fecha\":\"2017/2/27 7:6:21\",\"productos\":[{\"category_name\":\"Books\"}, {\"category_name\":\"Party & Occasions\"}]}");
        String fecha = object.get("fecha").textValue();
        //Formato que viene
        DateTimeFormatter a = DateTimeFormatter.ofPattern("yyyy/M/d H:m:s");
        LocalDateTime b = LocalDateTime.from(a.parse(fecha));
        
        //Formato que queremos
        DateTimeFormatter c = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        
        String date = b.format(c);
        
        Iterator<JsonNode> productos = object.get("productos").elements(); 
        
        System.out.println(object.get("productos").size());
        
        while (productos.hasNext()) {
            JsonNode producto = productos.next(); 
        	String categoryName = producto.get("category_name").toString();
            System.out.println(categoryName + ", "+ date);
        }
    }
}
