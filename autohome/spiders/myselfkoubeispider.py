# -*- coding: utf-8 -*-
import datetime

import scrapy
import time
from bs4 import BeautifulSoup
from scrapy.http import HtmlResponse
from selenium import webdriver

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

from crawlAutohomekoubei.items import KoubeiFailedItem, KoubeiItem
from crawlAutohomekoubei.pipelines import KoubeiPipeline, MongoDBPipeline,HBasePipelinef


class KoubeispiderSpider(scrapy.Spider):
    name = 'koubeispider'
    allowed_domains = ['k.autohome.com.cn']
    # start_urls = ['http://k.autohome.com.cn/4069/']
    # pipeline = set([HBasePipelinef, ])

    # start_urls = ['http://k.autohome.com.cn/']

    def start_requests(self):
        urls = [
           'http://k.autohome.com.cn/4069/',
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.urlParse)
            # yield scrapy.Request(url=url, callback=self.koubeiParse)

    def urlParse(self, response):
        soup = BeautifulSoup(response.text, 'lxml')
        titles = soup.findAll('div', class_='cont-title fn-clear')
        for title in titles:
            self._wait()
            url = title.find('a').attrs['href']
            yield scrapy.Request(url='http:' + url, callback=self.koubeiParse)
        next = response.css('div.mouth-cont.js-koubeidataitembox  div.page-cont  div  a.page-item-next').extract_first()
        print('next',next)
        if next is not None :
            nurl = next.strip() if next  else '';
            url = response.urljoin(nurl);
            self._wait()
            yield scrapy.Request(url=url, callback=self.parse);

        # logger.log(logging.INFO, '当前title %s' % title)

    def koubeiParse(self, response):
        try:
            yield self.koubeiHtml(response)
        except:
            item = KoubeiFailedItem(url=response.url)
            yield item

    def koubeiHtml(self, response):
        # print(response.text)
        soup = BeautifulSoup(response.text, 'lxml')

        # 口碑左侧信息
        mouthcon_cont_left = soup.find('div', class_='mouthcon-cont-left')
        # print('mouthcon_cont_left==',mouthcon_cont_left)
        item = KoubeiItem()
        item['url'] = response.url

        for dl_s in mouthcon_cont_left.findAll('dl'):
            item_name = dl_s.find('dt').get_text(strip=True)
            item_value = dl_s.find('dd').get_text(separator=' ',strip=True)
            if item.switcher.get(item_name):
                item[item.switcher.get(item_name)] = item_value

        ##有可能多个内容
        mouthcons = soup.findAll('div', class_='mouth-item')
        # koubei_content = mouthcons[0]
        for mouthcon in mouthcons:
            type = mouthcon.find('i', class_='icon icon-zj').get_text()
            if type == '口碑':
                koubei_content = mouthcon

        # 口碑主要内容信息
        item['date'] = koubei_content.find('div', class_='title-name name-width-01').find('b').get_text(strip=True)
        item['title'] = koubei_content.find('div', class_='kou-tit').find('h3').get_text(strip=True).lstrip("《").rstrip(
            '》')

        # 正文内容
        text_con = koubei_content.find('div', class_='text-con')
        replace_content_s_list = text_con.findAll('span')

        # 启动浏览器
        # browser = webdriver.PhantomJS()
        browser = webdriver.Chrome()
        browser.get(response.url)

        # 等待完成
        first_class = replace_content_s_list[0].attrs['class'][0]
        # print('first_class==',first_class)
        element_present = EC.presence_of_element_located((By.CLASS_NAME, first_class))
        WebDriverWait(browser, 60).until(element_present)

        # 字典，存储获取过的内容
        span_class_dict = {}
        for replace_span_s in replace_content_s_list:
            cls = replace_span_s.attrs['class'][0]
            if cls not in span_class_dict:
                script = "return window.getComputedStyle(document.getElementsByClassName('%s')[0],'before').getPropertyValue('content')" % (
                    cls)
                trans = browser.execute_script(script).strip('\"')
                span_class_dict[cls] = trans
            replace_span_s.replace_with(span_class_dict[cls])
        print(koubei_content)

        # 清除style和script
        [i.extract() for i in koubei_content.findAll('style')]
        [i.extract() for i in koubei_content.findAll('script')]

        item['content'] = koubei_content.get_text(strip=True)

        body = browser.page_source
        resp= HtmlResponse(body)
        # 主页名称
        # webname = resp.css(
        #     'body  div.content  div.subnav  div.subnav-title  div.subnav-title-name  a::text').extract_first()
        # # logger.log(logging.INFO, '当前页面名称 %s' % webname)
        #
        # # 标题
        # title = resp.xpath('//*[@id="0"]/dl/dd/ul/li[1]/text()').extract_first()
        # pscore = resp.xpath('//*[@id="0"]/dl/dd/ul/li[2]/span[1]/span[2]/text()').extract_first()
        # percount = resp.xpath('//*[@id="0"]/dl/dd/ul/li[2]/span[2]/text()').extract_first()
        # sunumber = resp.css('.supportNumber::text').extract_first()
        # lookcount = resp.css('div.mouth-main  div.mouth-remak  div.help  span:nth-child(4)  a::text').extract_first()
        #
        # item['sunumber'] = sunumber if sunumber else ''
        # item['lookcount'] = lookcount if lookcount else ''
        #
        # # 主页名称
        # item['webname'] = webname
        # # item['fromurl'] = from_url
        # # 标题
        # item['title'] = title
        # item['pscore'] = str(pscore)
        # item['percount'] = str(percount)

        nowtime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        item['crawldate'] = nowtime
        # browser.close()
        print('nowtime=',nowtime)
        self._wait()
        browser.close()
        return item

    def _wait(self):
        for i in range(0, 3):
            print('.' * (i % 3 + 1))
            time.sleep(0.1)