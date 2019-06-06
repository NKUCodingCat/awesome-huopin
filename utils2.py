
import itertools
import gzip
import json

def orderSensitiveKeysCheck(key):
    assert len(key) == 2
    return ''.join(key)

def orderInsensitiveKeysCheck(key):
    assert len(key) == 2
    return ''.join(sorted(key))

def danju2Keys(danju):
    # 单句的key是order-insensitive的
    return list(
            set(
                map(
                    lambda x: orderInsensitiveKeysCheck(''.join(x)),
                    itertools.combinations(list(danju), 2) 
            )))

def shuangju2Keys(sen1, sen2):
    # 双句的key是case-sensitve 的
    return list(set([
        orderSensitiveKeysCheck("%s%s"%(i, j))
        for i in sen1
        for j in sen2
    ]))

def mergeValue(data_from_redis, data_new):
    """
    data_from_redis: json string from redis, default None
    data_new: new data
    """
    if data_from_redis == None:
        data_from_redis = b"[]"
    else:
        data_from_redis = gzip.decompress(data_from_redis)

    o = set(json.loads(data_from_redis))
    ret = o|data_new # merge two set
    return gzip.compress(bytes(json.dumps(list(ret)), "ASCII"))

def parseRedisValue(redisRes):
    if redisRes == None:
        return []
    return json.loads(gzip.decompress(redisRes))