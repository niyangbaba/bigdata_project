package utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created by Administrator on 2019/1/8.
 */
public class Utils {

    public static Jedis getJedis() {
        JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), "h123", 6379);
        Jedis jedis = jedisPool.getResource();
        return jedis;
    }

}
