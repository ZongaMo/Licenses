from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import requests
import time
import pandas as pd
from bs4 import BeautifulSoup
import demjson3 as demjson

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='script.log'
)
# session = requests.Session()
timeout = 20
url={
    "home":"https://xkz.nfra.gov.cn/jr/NWkWkC/getLicence.do",
    "detail":"https://xkz.nfra.gov.cn/jr/NWkWkC/showLicenceInfo.do",
    "organType":"https://xkz.nfra.gov.cn/jr/getOrganType.do",
}
params= {
    "home":{
        "useState": "3",
        "organNo": "",
        "fatherOrganNo": "",
        "province": "",
        "orgAddress": "",
        "organType": "",
        "branchType": "",
        "fullName": "",
        "address": "",
        "flowNo": "",
        "jrOrganPreproty": "",
    },
    "detail":{
        "id": "",
    },
    "organType":{
        "_dc": int(time.time() * 1000),
    }
}
headers = {
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    # "Content-Length": "10",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Cookie": "isClick=true; JSESSIONID=0000XRGqh-5uOFP1Y6nbs4UjnFv:-1",
    "Host": "xkz.nfra.gov.cn",
    "Origin": "https://xkz.nfra.gov.cn",
    "Pragma": "no-cache",
    "Referer": "https://xkz.nfra.gov.cn/jr/",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "sec-ch-ua": "\"Not(A:Brand\";v=\"99\", \"Microsoft Edge\";v=\"11\", \"Chromium\";v=\"11\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/11.0.0.0 Safari/537.36 Edg/11.0.0.0",
    "X-Requested-With": "XMLHttpRequest",
}
dat = {
    "start": "0",
    "limit": "10",
}


# class ProxyRotator:
#     def __init__(self, api_url='https://proxy.scdn.io/api/get_proxy.php', max_retry=3):
#         self.api_url = api_url
#         self.proxies = []
#         self.current_index = 0
#         self.max_retry = max_retry
#         self._refresh_proxies()
#
#     def _call_api(self):
#         """带重试机制的API调用"""
#         api_params = {'protocol': 'socks4', 'count': 20}
#         for _ in range(self.max_retry):
#             try:
#                 response = requests.get(self.api_url, params=api_params, timeout=5)
#                 if response.ok:
#                     data = response.json()
#                     if data['code'] == 200:
#                         return data['data']['proxies']
#             except Exception as e:
#                 print(f"API Error: {str(e)}")
#         return []
#
#     def _refresh_proxies(self):
#         """刷新代理池并重置指针"""
#         new_proxies = self._call_api()
#         if new_proxies:
#             self.proxies = new_proxies
#             self.current_index = 0
#         else:
#             raise Exception("代理获取失败")
#
#     def get_next_proxy(self):
#         self.current_index += 1
#
#     def get_proxy(self):
#         """
#         智能获取代理
#         :return: 代理IP字符串
#         """
#         # 指针越界时自动刷新
#         if self.current_index >= len(self.proxies):
#             self._refresh_proxies()
#             # return self.proxies[self.current_index]
#             return {"https":f"socks4://{self.proxies[self.current_index]}"}
#
#         # 确保索引有效
#         while self.current_index < 0 or self.current_index >= len(self.proxies):
#             self._refresh_proxies()
#
#         # return self.proxies[self.current_index]
#         return {"https":f"socks4://{self.proxies[self.current_index]}"}

def get_detail(row):
    times = 0
    retry = False
    # time.sleep(.2)
    result = {"机构编码": row["certCode"],
                "机构名称": row["fullName"],
                "流水号": row["flowNo"],
                "批准日期": row["setDate"],
                "发证日期": row["printDate"],
                "id": row["id"]}
    while True:
        try:
            times += 1
            response = requests.get(url["detail"]+f"?id={row["id"]}",
                                   headers=headers,
                                   # proxies=rotator.get_proxy(),
                                    timeout=timeout
                                   ) ####
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text, 'lxml').find('table', class_='trw-table-s1')
            if "您的操作过于频繁，请五分钟后再试" in soup.get_text():
                print(f"拉取频繁,id{row["id"]}, 第{times}次尝试连接, 正在重试")  ####
                if times >= 5:
                    time.sleep(100)
                continue
        except:
            print(f"请求失败,id{row["id"]}, 第{times}次尝试连接")  ####
            continue
        is_pre_section = False  # 标记是否处于变更前信息段落
        for tr in soup.find_all('tr'):
            # 检测变更前信息段落开始
            if tr.th and "变更前机构信息" in tr.th.get_text():
                is_pre_section = True
                continue
            # 处理数据行
            if 'a0' in tr.get('class', []):
                tds = tr.find_all('td')
                if len(tds) == 2:
                    key = tds[0].get_text(strip=True).replace('：', '')
                    value = tds[1].get_text(strip=True)
                    # 添加前缀规则
                    final_key = f"变更前_{key}" if is_pre_section else key
                    result[final_key] = value
        if result["发证机关"]=="":
            continue
        response.close()
        return result
    logging.error(f"【异常终止】id{row["id"]}获取失败")


# rotator = ProxyRotator()
def get_list(start = 0):
    dat = {"start": start, "limit": 10}
    times = 0
    # retry = False
    # time.sleep(.1)
    while True:
        try:
            times += 1
            homePage = requests.post(url["home"],
                                    params=params["home"],
                                    data=dat,
                                    headers=headers,
                                    # proxies=rotator.get_proxy(),
                                     timeout=timeout
                                    ) ####
            homePage.encoding = 'utf-8'
            # print(dat, homePage.text) ####
            pageData = homePage.json()
            if pageData["success"] is not True:
                # print(pageData["msg"],f"当前代理{rotator.get_proxy()}")
                print(pageData["msg"]) ####
                time.sleep(60)
                continue
            homePage.close()
        except:
            # print(f"报错重试中,当前代理{rotator.get_proxy()}") ####
            # rotator.get_next_proxy()
            print(f"列表拉取出错，重试中，第{times+1}次") ####
            continue
        # if pageData["success"] != "true":
        #     rotator.get_next_proxy()
        #     continue
        time_now = time.strftime("%X", time.localtime())
        print(f"{time_now}  当前位置{start}-{start+10}")
        # list.append(pd.DataFrame(pageData["datas"]))
        return pageData["datas"], pageData["total"]
    # print(f"获取列表失败,start:{start}") ####
    logging.error(f"获取列表失败,start:{start}")

def extract_list_data(category, list_data, res):
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(get_detail, row) for row in list_data]
        for future in as_completed(futures):
            detail = future.result()
            detail["机构类型"] = category["orgTypeName"]
            time_now = time.strftime("%X", time.localtime())
            print(f"{time_now}请求成功,id{detail["id"]}")  ####打印时间和id
            res.append(detail)

# 按装订区域中的绿色按钮以运行脚本。
if __name__ == '__main__':
    # home = requests.get("https://xkz.nfra.gov.cn/jr/",
    #                     headers=headers,
    #                     timeout=timeout)
    # headers["Cookie"] = f"isClick=true; JSESSIONID={home.cookies.get_dict()['JSESSIONID']}"
    organType = requests.get(url["organType"],
                            headers=headers,
                            timeout=timeout)
    categories = [demjson.decode(organType.text)['root'][i] for i in [3]] # 3，6
    for category in categories:
        res = []
        params["home"]["organType"] = category["orgTypeCode"]
        _, total = get_list()
        # total = 1000
        print(f"类别{category["orgTypeName"]}总数{total}")
        for begin in range(20000, total, 20000):
            end = begin + 20000 if begin + 20000 < total else total
            starts = [start for start in range(begin, end, 10)]
            listDatas = []
            res = []
            with ThreadPoolExecutor(max_workers=8) as executor:
                futures = [executor.submit(get_list, start) for start in starts]
                for future in as_completed(futures):
                    listData, _ = future.result()
                    listDatas.extend(listData) # 合并列表
            pd.DataFrame(listDatas).to_excel(f"idList{category["orgTypeName"]}_{begin//20000}.xlsx", index=False)
            # 解析列表数据
        # begin = 0
        # listDatas = pd.read_excel(f"idList{category["orgTypeName"]}_{begin//20000}.xlsx", dtype={"flowNo": str}).to_dict(orient="records")
            extract_list_data(category, listDatas, res)
            pd.DataFrame(res).to_excel(f"data_{category["orgTypeName"]}_{begin//20000}.xlsx",
                                       index=False,
                                       sheet_name=category["orgTypeName"])
            time.sleep(120)
    # home.close()
    # session.close()


