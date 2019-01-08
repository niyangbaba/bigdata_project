package dao.daoImpl;

import dao.FlowStatDao;
import domain.FlowStat;
import redis.clients.jedis.Jedis;
import utils.Utils;

import java.util.ArrayList;
import java.util.Map;

/**
 * Created by Administrator on 2019/1/8.
 */
public class FlowStatDaoImp implements FlowStatDao {
    @Override
    public ArrayList<FlowStat> getUv() {

        ArrayList<FlowStat> flowStats = new ArrayList<FlowStat>();
        Jedis jedis = Utils.getJedis();

        Map<String, String> ucCountMap = jedis.hgetAll("uv");

        for (Map.Entry<String, String> kv : ucCountMap.entrySet()) {
            flowStats.add(new FlowStat(kv.getKey(),kv.getValue()));
        }
        jedis.close();
        return flowStats;
    }
}
