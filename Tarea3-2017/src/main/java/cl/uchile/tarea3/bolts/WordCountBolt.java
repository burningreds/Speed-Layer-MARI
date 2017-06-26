package cl.uchile.tarea3.bolts;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class WordCountBolt extends BaseBasicBolt {
	Map<String, Long> counts = new HashMap<String, Long>();

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String word = tuple.getString(0);
		Long count = counts.get(word);
		if (count == null)
			count = (long) 0;
		count++;
		counts.put(word, count);
		System.out.println("Category: "+word+": "+count);
		collector.emit(new Values(word, count));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("category", "count"));
	}
}