/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cl.uchile.tarea3.queries;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.io.FileWriter;
import java.util.UUID;

/**
 * Q5 Sucursal locations: Selects sucursales and their location and 
 * generates json file for d3 visualizations
 * 
 * @author Paula
 *
 */
public class SucursalLocations {

	/**
	 * @param args
	 *            the command line arguments
	 */
	public static void main(String[] args) {

		Cluster cluster;
		Session session;

		cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
		session = cluster.connect("tarea3");

		int sucursalId = 0;
		double lng = 0;
		double lat = 0;

		// Create the node factory that gives us nodes.
		JsonNodeFactory factory = new JsonNodeFactory(false);

		// create a json factory to write the treenode as json. for the example
		// we just write to console
		JsonFactory jsonFactory = new JsonFactory();
		ObjectMapper mapper = new ObjectMapper();

		// the root node - album
		ArrayNode sucursales = factory.arrayNode();

		ResultSet results = session.execute("SELECT * FROM sucursal_location");

		for (Row row : results) {
			sucursalId = row.getInt("sucursal_id");
			lng = row.getDouble("lng");
			lat = row.getDouble("lat");

			JsonNode sucursal = factory.objectNode()
					.put("id", sucursalId)
					.put("lng", lng)
					.put("lat", lat);
			sucursales.add(sucursal);

			// sucursales.;
			System.out.println(sucursalId + ": " + lng + ", " + lat);
		}

		try (FileWriter file = new FileWriter("locations.json")) {
			com.fasterxml.jackson.core.JsonGenerator generator = jsonFactory.createGenerator(file);
			mapper.writeTree(generator, sucursales);
			System.out.println("Json file succesfully created");
		} catch (Exception e) {
			e.printStackTrace();
		}

		cluster.close();
	}

	static final long NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L;

	public static long getTimeFromUUID(UUID uuid) {
		return (uuid.timestamp() - NUM_100NS_INTERVALS_SINCE_UUID_EPOCH) / 10000;
	}

}
