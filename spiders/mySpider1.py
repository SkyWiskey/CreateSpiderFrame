import aiohttp
from lxml import etree

spiderSettings ={
        # redis配置
        "taskName":"company_info",
        "redisHost":'192.168.0.102',
        "redisPort":6379,
        "redisDB":0,

        # mongodb配置
        'mongoHost':'127.0.0.1',
        'mongoPort':27017,
        'dbName' : 'baidu_test' , # mongodb数据库名称,
        'cltName' : 'title_list' ,# mongodb集合名称,

        'multiProNums' : 1 , # 爬虫进程的个数
        'concurrentCount' : 3   ,         # 并发数量
}


async def getResponse(urlTask,queResponse):
    """ 并发请求url任务
    :param queResponse:  请求url返回response的队列
    :return:
    """
    # 假如 urlTask是一个url
    url = urlTask
    # 如果是个id 我们需要构造url
    # 比如 url = f"https://www.xxx.com/distract?cid={urlTask}"
    try:
        async with aiohttp.ClientSession()as cs:
            async with cs.get(url) as resp:
                # 错误判断
                if resp.status == 200:
                    text = await resp.text()
                    queResponse.put((urlTask,text))
    except:
        pass

def parseResponse(urlTask,text):
    """
    解析text 可以使用xpath 或者正则表达式来提取item
    :param text: 网页响应结果
    :return: dict
    """
    item = dict(_id = urlTask)
    html = etree.HTML(text)
    titleList = html.xpath("//div[@id='s-top-left']/a/text()")
    item['titleList'] = titleList
    print(item)
    return item