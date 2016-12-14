package newborn_town.pspm.online.bolt;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * 将pspm click原始日志根据 campaign_id + publisher_id + click_ua + click_ip 进行分组
 * 
 * @author yangyang
 *
 */
public class PSPMClickLogGroupingBolt extends BaseBasicBolt {

	private static final long serialVersionUID = -6707182554696236571L;

	static Logger logger = Logger.getLogger(PSPMClickLogGroupingBolt.class);

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {

		String clicklog = input.getString(0) + "|";

		String[] sentence = clicklog.split("\\|");

		if (sentence.length < 25) {
			return;
		}

		String create_time = sentence[1];
		String campaign_id = sentence[2];
		String publisher_id = sentence[7];
		String click_ua = sentence[21];
		String click_ip = sentence[23];

		if (StringUtils.isEmpty(create_time) || create_time.length() != 19)
			return;

		String groupingStr = campaign_id + publisher_id + click_ua + click_ip; // 需要分组的字段

		collector.emit(new Values(groupingStr, sentence));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// ("分组的字段","分割后的日志字段数组")
		declarer.declare(new Fields("clickgroupingStr", "logs"));
	}
}
