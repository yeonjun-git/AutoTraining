# Naver news Crawling Module
import sys
import os
module_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
sys.path.append(module_path)


import aiohttp
import asyncio
from Module.config import get_secret

class NaverNews:

    url = "https://openapi.naver.com/v1/search/news"
    naver_id = get_secret("naver_api_id")
    naver_pw = get_secret("naver_api_pw")

    def unit_url(self, keyword, display, start):
        return {
            "url" : f"{self.url}?query={keyword}&display={display}&start={start}",
            "headers" : {
                            "X-Naver-Client-Id" : self.naver_id,
                            "X-Naver-Client-Secret" : self.naver_pw
                        }
        }
    
    @staticmethod
    async def fetch(session, url, headers):
        async with session.get(url, ssl = False, headers = headers) as response:
            if response.status == 200:
                result = await response.json()
                return result['items']

    async def search(self, keyword, total_page, display):
        apis = [self.unit_url(keyword, display, 1+i*10) for i in range(total_page)]
        async with aiohttp.ClientSession() as session:
            all_data = await asyncio.gather(
                *[NaverNews.fetch(session, api["url"], api["headers"]) for api in apis]
            )
        return all_data

    def run(self, keyword, total_page, display):
        return asyncio.run(self.search(keyword, total_page, display))
    

class Dataset:

    def DataList(self, keyword, page, per_page):
        crawling = NaverNews()
        data = crawling.run(keyword, page, per_page)
        title, link, desc, pubdate = [], [], [], []
        for i in range(page):
            for j in range(per_page):
                title.append(data[i][j]['title'])
                link.append(data[i][j]['link'])
                desc.append(data[i][j]['description'])
                pubdate.append(data[i][j]['pubDate'])
        
        return title, link, desc, pubdate

                

if __name__ == "__main__":
    check = Dataset()
    check.DataList("금리", 1, 1)