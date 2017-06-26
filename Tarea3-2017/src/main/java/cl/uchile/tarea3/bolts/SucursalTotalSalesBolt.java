package cl.uchile.tarea3.bolts;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class SucursalTotalSalesBolt extends BaseBasicBolt {
	Map<Integer, Double> totalSum = new HashMap<Integer, Double>();

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		int sucursal = tuple.getIntegerByField("sucursal");
		String addr = tuple.getStringByField("addr");
		double totalBoleta = tuple.getDoubleByField("total");
		Double total = totalSum.get(sucursal);
		if (total == null)
			total = 0.0;
		total += totalBoleta;
		totalSum.put(sucursal, total);
		total = total * 100;
		//se guarda el valor amplificado para guardar en long y hacer sort
		collector.emit(new Values(sucursal, total.longValue(), addr));
		//System.out.println(total.longValue()+addr);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sucursal", "count", "addr"));
	}
}