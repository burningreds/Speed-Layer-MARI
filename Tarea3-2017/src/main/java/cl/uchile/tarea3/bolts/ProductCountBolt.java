package cl.uchile.tarea3.bolts;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class ProductCountBolt extends BaseBasicBolt {
	Map<Integer, Long> counts = new HashMap<Integer, Long>();

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		int itemId = tuple.getIntegerByField("itemId");
		String name = tuple.getStringByField("name");
		Long count = counts.get(itemId);
		if (count == null)
			count = (long) 0;
		count++;
		counts.put(itemId, count);
		collector.emit(new Values(itemId, count, name));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("item", "count", "name"));
	}
}