"""
实现多引擎模式异步并发爬虫系统
三大引擎：
    任务引擎，爬虫引擎，解析引擎
"""
import sys

import redis
import multiprocessing
import asyncio
import pymongo
import importlib


def redisRunning(taskUrls,redisCli,concurrentCount,queRequest,multiProNums):
    """ redis任务制造引擎模块
    :param taskUrls: 存放url任务的集合key名称
    :param concurrentCount: 返回url任务的数量
    :param queRequest: 多进程队列
    :return:
    """
    # 缓存集合
    taskUrlsBuffer = taskUrls + "_buffer"
    # 指纹集合
    taskUrlsFp = taskUrls + '_fp'
    while redisCli.scard(taskUrls) > 0:
        # 首先判断缓存集合是否存在 exists 存在返回1 不存在返回0
        if not redisCli.exists(taskUrlsBuffer):
            # 将原始url任务集合中的任务copy到url缓存中
            redisCli.sunionstore(taskUrlsBuffer,taskUrls)

        while True:
            #开始从缓存任务集合中提取url任务 count 代表并发的数量
            urlTaskList = redisCli.spop(taskUrlsBuffer,count=concurrentCount)
            if len(urlTaskList) == 0:
                break
            # 真正要放入队列的任务集合，去过重的,请求成功过的url不再放入队列
            urlTaskListX = list()

            # 先判断一下 任务在不在指纹集合当中
            for urlTask in urlTaskList:
                # 指纹不存在
                if not redisCli.sismember(taskUrlsFp,urlTask):
                    urlTaskListX.append(urlTask)
                else:
                    # 如果指纹存在 删除原始任务集合中的任务
                    redisCli.srem(taskUrls,urlTask)
            if len(urlTaskListX)>0:
                queRequest.put(urlTaskList)
    # 当原始任务集合中的任务没有了 在队列中放入None
    for i in range(multiProNums):
        queRequest.put(None)

async def spiderStart(*spiders):
    """
    并发运行写成函数的最高层级入口
    :param spiders: 任务
    :return:
    """
    await asyncio.gather(*spiders)

def spiderEngine(queRequest,getResponse,queResponse):
    """ 爬虫请求模块
    :param queRequest: 任务队列
    :return:
    """
    while True:
        # 从任务队列中提取要爬取的url任务
        urlTaskList = queRequest.get()
        if urlTaskList is not None:
            # 发送任务请求  asyncio aiohttp
            # 先构造并发的请求函数 ，为协程
            # 定义一个并发运行协程请求函数的最高层级入口点
            # spiders = *[getResponse() for i in range(10)]
            spiders = [getResponse(urlTask,queResponse) for urlTask in urlTaskList]
            # spiderStart(*spiders)

            # 并发
            asyncio.run(spiderStart(*spiders))
            # loop = asyncio.get_event_loop()
            # loop.run_until_complete(spiderStart(*spiders))
        else:
            return

def save_to_mongodb(mongoCli,dbName,cltName,item):
    """
    将数据存储到 MongoDB中
    :param item:
    :return:
    """
    collection = mongoCli[dbName][cltName]
    try:
        if item:
            collection.insert_one(item)
    except:
        pass

def parseReponseEngine(mongoCli,taskUrls,queResponse,parseResponse,dbName,cltName):
    """
    解析response的引擎
    :param queResponse: response队列
    :return:
    """
    taskUrlsFp = taskUrls + '_fp'
    while True:
        queGet = queResponse.get()
        # 结束响应处理的标志 None
        if queGet is None:
            break
        urlTask,text = queGet
        # 解析网页响应结果text
        # item = parseResponse(urlTask,text)
        for item in parseResponse(urlTask,text):

            # 存储数据到MongoDB中
            save_to_mongodb(mongoCli,dbName,cltName,item)
        # 移除处理过的url任务
        redisCli.smove(taskUrls,taskUrlsFp,urlTask)

def multiMain(redisCli,mongoCli,settingsData,getResponse,parseResponse,parseReponseEngine):
    dbName = settingsData['dbName']  # mongodb数据库名称
    cltName = settingsData['cltName'] # mongodb集合名称
    multiProNums = settingsData['multiProNums'] # 爬虫进程的个数
    taskUrls = settingsData['taskUrls']             # 存放原始url任务的集合
    concurrentCount = settingsData['concurrentCount']           # 并发数量

    queRequest = multiprocessing.Queue(maxsize=multiProNums*2)   # url任务队列
    # 启动redis任务制造引擎为了防止阻塞 采用单独的进程
    redis_running_process = multiprocessing.Process(target=redisRunning,
                                        args=(taskUrls,redisCli,concurrentCount,queRequest,multiProNums))
    redis_running_process.run()

    # 启动爬虫引擎 需要联网 所以开启多个进程
    queResponse = multiprocessing.Queue()  # response队列
    pList = [multiprocessing.Process(target=spiderEngine,args=(queRequest,getResponse,queResponse),
                                        name=f'{i}') for i in range(multiProNums)]
    [p.run() for p in pList]

    # 启动解析引擎
    parse_engine_process = multiprocessing.Process(target=parseReponseEngine,
                                        args=(mongoCli,taskUrls,queResponse,parseResponse,dbName,cltName))
    parse_engine_process.run()
    # 阻塞等待url任务原始集合里面的url任务消耗完毕
    redis_running_process.join()
    [p.join() for p in pList]
    queResponse.put(None)

def getMyspider():
    """
    动态获取爬虫文件的变量
    :return:
    """
    assert len(sys.argv) >= 2, '请输入正确的命令格式 : python main.py <爬虫文件>'
    spiderName = sys.argv[1].split('\\')[-1].replace('.py', '')
    spider = importlib.import_module(f'spiders.{spiderName}')
    settingsData = spider.settingsData
    getResponse = spider.getResponse
    parseResponse = spider.parseResponse
    return settingsData,getResponse,parseResponse

if __name__ == '__main__':
    # 程序运行方式为 python main.py <爬虫文件>
    # 添加爬虫功能的思路，比如代理ip，哪里需要就往哪里添加，一层层的向上
    settingsData, getResponse, parseResponse = getMyspider()
    redisCli = redis.Redis(host=settingsData['redisHost'],port=settingsData['redisPort'],
                           db=settingsData['db'],decode_responses=True)
    mongoCli = pymongo.MongoClient(host=settingsData['mongoHost'], port=settingsData['mongoPort'],
                                   # 实例化mongoCLi的时候，不进行链接
                                   connect=False,)
    # 程序入口
    multiMain(redisCli,mongoCli,settingsData,getResponse,parseResponse,parseReponseEngine)