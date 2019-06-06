import itertools
import json
import gzip

#key 包含两个字，按utf8序排列
def checkKey(key):
    '''
    Convert key to unified sorted key, in utf8
    '''

    assert len(key) == 2
    return ''.join(sorted(key))

def danju2Keys(danju):
    return list(
            set(
                map(
                    lambda x: checkKey(''.join(x)),
                    itertools.combinations(list(danju), 2) 
            )))

def shuangju2Keys(sen1, sen2):
    return list(set([
        checkKey("%s%s"%(i, j))
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

if __name__ == "__main__":
    assert checkKey("知乎") == checkKey("乎知") 
    assert "乎知" != "知乎"
    assert checkKey("知乎") == "乎知"
    print(danju2Keys("一生一世一双人"))
    print(shuangju2Keys("一山一水中一寺", "一林黄叶一僧归"))

    