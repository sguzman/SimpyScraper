import redis

base = "http://23.95.221.108/page/"
redisHash = 'ebooks'

red = redis.StrictRedis()
hashmap = red.hgetall(redisHash)
limit = 1268