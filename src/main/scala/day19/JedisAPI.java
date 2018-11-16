package day19;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * creat by newforesee 2018/10/17
 */
public class JedisAPI {
    public static void main(String[] args) {
        //创建JedisPool
        JedisPool jedisPool = new JedisPool("master", 6379);
        //通过pool获取jedis实例
        Jedis jedis = jedisPool.getResource();
        jedis.auth("123");

        jedis.flushDB();
        /**
         * String字符串类型
         */
        jedis.set("bigdata","db");
        //在原来的key对应的value值追加字符串
        //如果key不存在则创建一个
        jedis.append("bigdata","text");
        System.out.println(jedis.get("bigdata"));
        //自增 进行累加操作 等价于count++默认值时0
        jedis.incr("count");
        System.out.println(jedis.get("count"));
        //加指定数值
        jedis.incrBy("count",10);
        System.out.println(jedis.get("count"));
        //进行浮点类型运算,没又double,只有float
        jedis.incrByFloat("double",1.5);
        System.out.println(jedis.get("double"));
        //减法
        jedis.decr("count");
        System.out.println(jedis.get("count"));
        jedis.decrBy("count",5);
        System.out.println(jedis.get("count"));
        //更改指定key的值
        jedis.getSet("count","100");
        System.out.println(jedis.get("count"));
        //获取指定范围内的字符串 包头包尾 ▄︻┻┳═一…… ☆(>○<)
        System.out.println(jedis.getrange("bigdata", 0, 3));
        //替换
        jedis.setrange("bigdata",0,"php");
        System.out.println(jedis.get("bigdata"));
        //只有key不存在的时候保存
        Long r = jedis.setnx("counts", "200");
        System.out.println(r);
        System.out.println(jedis.get("counts"));
        jedis.mset("java","html","c++","ss");
        List<String> mget = jedis.mget("java","c++");
        System.out.println(mget);
        System.out.println("=============================");
        /**
         * 集合操作
         * Set 元素无序不可重复用
         */
        //添加元素
        long s = jedis.sadd("hobbys", "吃", "喝", "读书", "睡觉");
        System.out.println(s);
        long size = jedis.scard("hobbys");
        System.out.println(size);
        //取多个集合的差集
        jedis.sadd("hobbys2","lvyou","dapai","吃");
        Set<String> sdiff = jedis.sdiff("hobbys", "hobbys2");
        System.out.println(sdiff);

        jedis.sdiffstore("hobby","hobbys", "hobbys2");//???????????????
        System.out.println(jedis.scard("hobby"));

        //交集
        Set<String> sinter = jedis.sinter("hobbys", "hobbys2");
        System.out.println(sinter);
        //判断某个元素是否在集合中
        Boolean sismember = jedis.sismember("hobbys", "吃");
        System.out.println(sismember);
        //取集合中的元素
        Set<String> hobbys = jedis.smembers("hobbys");
        System.out.println(hobbys);
        //从集合中随机取元素
        List<String> hobbys1 = jedis.srandmember("hobbys", 3);
        System.out.println(hobbys1);
        //从集合中移除元素,返回删除的个数
        Long srem = jedis.srem("hobbys", "吃");
        System.out.println(srem);
        //根据查询 搜索集合中的元素 搜索开始位置0  搜索规则
        jedis.sadd("hobbys","java","javascript","php","c","c#","c++");
        ScanParams scanParams = new ScanParams();
        scanParams.match("c*");
        ScanResult<String> result = jedis.sscan("hobbys", 0, scanParams);
        List<String> list = result.getResult();
        System.out.println(list);
        /**
         * ZSet
         * Sorted 类型
         * 元素不可重复
         * 有序(根据score排序)
         * score值越大越靠后
         */
        jedis.zadd("price",1,"10");
        jedis.zadd("price",5,"1");
        jedis.zadd("price",2,"3.14");
        jedis.zadd("price",6,"99");
        jedis.zadd("price",3,"16");
        jedis.zadd("price",10,"500");

        //获取元素列表
        Set<String> price = jedis.zrange("price", 0, -1);
        System.out.println(price);
        //获取指定score范围的元素数量
        Long zcount = jedis.zcount("price", 1, 8);
        System.out.println(zcount);
        //指定key在集合中的分值排名,从0开始
        Long zrank = jedis.zrank("price", "10");
        System.out.println(zrank);
        //删除元素,删除成功会返回删除的数量,失败返回0
        Long zrem = jedis.zrem("price", "500", "99");
        System.out.println(zrem);
        //获取元素排名分值
        Double zscore = jedis.zscore("price", "16");
        System.out.println(zscore);
        //获取指定范围的后几个元素
        Set<String> zrevrange = jedis.zrevrange("price", 0, 3);
        System.out.println(zrevrange);
        /**
         * List
         * 队列 list
         */
        //向头添加元素返回链表长度
        Long lpush = jedis.lpush("users","linda", "newforesee", "wety", "five");
        System.out.println(lpush);
        //向尾部添加元素返回链表长度
        Long rpush = jedis.rpush("users", "mayun", "mahuateng", "likaifu");
        System.out.println(rpush);
        //获取链表长度
        Long llen = jedis.llen("users");
        System.out.println(llen);
        //获取链表元素 指定范围
        List<String> users = jedis.lrange("users", 0, -1);
        System.out.println(users);
        //删除指定的相同元素数量,返回删除的元素数量
        jedis.rpush("users","mayun","mayun");
        System.out.println(jedis.lrange("users", 0, -1));
        Long lrem = jedis.lrem("users", 2, "mayun");
        System.out.println(jedis.lrange("users", 0, -1));
        //往队列头部插入元素,列表必须存在
        jedis.lpushx("users","huangbo");
        //修改队列中指定索引的元素
        String lset = jedis.lset("users", 0, "jety");
        System.out.println(jedis.lrange("users", 0, -1));
        //获取指定索引的元素
        String lindex = jedis.lindex("users", 0);
        System.out.println(lindex);
        /**
         * hash散列
         */
        //添加元素如果key存在则修改
        Long username = jedis.hset("user", "name", "zhangsan");
        Long userage = jedis.hset("user", "age", "18");
        System.out.println(username);
        System.out.println(userage);
        //获取某一个key的值
        String hget = jedis.hget("user", "name");
        System.out.println(hget);
        //获取所有值
        Map<String, String> user = jedis.hgetAll("user");
        for (Map.Entry<String, String> stringStringEntry : user.entrySet()) {
            System.out.println(stringStringEntry.getKey() + "=" + stringStringEntry.getValue());
        }
        //判断元素是否存在

        Boolean hexists = jedis.hexists("user", "sex");
        //获取hash
        //获取所有字段的值
        List<String> user1 = jedis.hvals("user");
        System.out.println(user1);
        //删除字段
        Long hdel = jedis.hdel("user", "name", "age");
        //查找字段
        jedis.hset("user","js1","value1");
        jedis.hset("user","xxxjs1","value2");
        jedis.hset("user","jas1","value3");
        jedis.hset("user","abcjs1","value4");
        ScanParams scanParams1 = new ScanParams();
        scanParams1.match("*js1");
        ScanResult<Map.Entry<String, String>> hscan = jedis.hscan("user", 0, scanParams1);
        for (Map.Entry<String, String> stringStringEntry : hscan.getResult()) {
            //
        }


        jedis.close();
    }
}
