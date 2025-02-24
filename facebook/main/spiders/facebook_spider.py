
from datetime import datetime
import sqlite3
from scrapy.http import HtmlResponse
import scrapy
import json
import requests
import re
import time
import decimal
import os
from scrapy.utils.project import get_project_settings



class GetAll(scrapy.Spider):
    name = 'get_all'
#    allowed_domains = ['https://www.digikey.com/products/en/capacitors/ceramic-capacitors/60/']
#    start_urls = ('https://www.digikey.com/products/en/integrated-circuits-ics/memory/774', )
#    start_urls = ('https://www.digikey.com/products/en/integrated-circuits-ics/memory/774?page=1&pageSize=500', )

    start_time=datetime.now()

    group_id=[ '443709852472133'

                ]


    def start_requests(self):
        #取得所有廠牌
        for gid in self.group_id:
            url=f"https://www.facebook.com/groups/{gid}/?sorting_setting=CHRONOLOGICAL"
            yield scrapy.Request(url, self.parse,meta={'page_type':'group','group_id':gid})


    def parse(self, response):
        #response is json
        #取得所有貼文
        data=json.loads(response.body)
        data['group_id']=response.meta['group_id']
        if data.get('post_data'):
            yield data
      


class GetPOST(scrapy.Spider):
    name = 'get_post'

    settings = get_project_settings()    
    db = settings.get('SQLITE_DB_NAME')
    db_conn =sqlite3.connect(db)
    db_cur = db_conn.cursor()


    def start_requests(self):
        self.db_cur.execute(f"SELECT group_id,post_id FROM fb_posts where author_id is null")
        self.post_list= list(self.db_cur.fetchall())
        #取得所有Post
        for item in self.post_list:
            url=f"https://www.facebook.com/groups/{item[0]}/posts/{item[1]}/"
            yield scrapy.Request(url, self.parse,meta={'page_type':'post','group_id':item[0],'post_id':item[1]})
            

    def parse(self, response):
        #Post作者

        tmp_post_author=response.xpath('//div[@data-ad-rendering-role="profile_name"]//a')[0] if response.xpath('//div[@data-ad-rendering-role="profile_name"]//a') else None
        # "https://www.facebook.com/groups/443709852472133/user/100002546332989/?__cft__[0]=AZXQ5Y8hvYtD8gsSvAbkj36Nq1klaxeh_fYPRGcaATLm0UfyBQ3fEuzegbz3KemJv-saJtXChDrQ7ffEYMgQohY29fSbXOG-vUY7to70L43t4IXlPHdm1zY68SpRAuKlitn4uWWmIeyxPpcACGa5JudeenwOF_vi0z3qUUyiHRmGFQ&__tn__=-]C%2CP-R"
        if tmp_post_author:
            post_author_id=re.search(r'user/(\d+)', tmp_post_author.xpath('./@href').get()).group(1)
            post_author_name=tmp_post_author.xpath('./text()[normalize-space()]').get()
            post_text="\n".join(response.xpath('//div[@data-ad-rendering-role="story_message"]//text()').extract())
            t_timestamp=response.meta['t_timestamp']

            yield({
                'group_id':response.meta['group_id'],
                'post_id':response.meta['post_id'],
                'author_id':post_author_id,
                'message':post_text,
                't_timestamp':t_timestamp
            })
            yield({
                'author_id':post_author_id,
                'author_name':post_author_name
            })
            #comment
            tmp_comments=response.xpath('//div[@class="html-div x11i5rnm xat24cr x1mh8g0r xexx8yu x4uap5 x18d9i69 xkhd6sd x1gslohp"]/div[not(@class)]')
            for item in tmp_comments:
               
                tmp_parse=item.xpath(".//div[@class='x1r8uery x1iyjqo2 x6ikm8r x10wlt62 x1pi30zi']")
                tmp_comment=tmp_parse[0]
                tmp_comment_author=tmp_comment.xpath('.//a[contains(@href,"/user/")]')[0]
                comment_author_id=re.search(r'user/(\d+)', tmp_comment_author.xpath('.//@href').get()).group(1)
                comment_author_name=tmp_comment_author.xpath('.//text()[normalize-space()]').get()

                comment_text="\n".join(tmp_comment.xpath('.//div[@class="x1lliihq xjkvuk6 x1iorvi4"]//text()[normalize-space()]').extract())
                tmp_comment_id=tmp_comment.xpath(".//li[@class='html-li xdj266r xat24cr xexx8yu x4uap5 x18d9i69 xkhd6sd x1rg5ohu x1emribx x1i64zmx']//a/@href").get()
                #https://www.facebook.com/groups/443709852472133/posts/2998207153689044/?comment_id=2998961160280310&__cft__[0]=AZWv-ZyTAxQycl1EBdPs7BAa-JJadvWon68g1Tw6rxDbBBJxCvN-7dHIircS1tKHLNzV6BHW54_Kx0-npUMUlmfAsk_Q3-i2Ao9seI6zEMLo73PbDKmkRlIrwrSXrsImVAeIn8kRdgflEvvsPH-XkdCnPiKNa9QYXwOaK1PBTWMm3Q&__tn__=R]-R
                comment_id=re.search(r'comment_id=(\d+)', tmp_comment_id).group(1)
                
                
                yield({
                    'post_id':response.meta['post_id'],
                    'comment_id':comment_id, 
                    'author_id':comment_author_id,
                    'message':comment_text,
                    't_timestamp':None
                })

                yield({
                    'author_id':comment_author_id,
                    'author_name':comment_author_name,
                })

                tmp_replies=tmp_parse[1:]

                for item2 in tmp_replies:
                    tmp_reply_author=item2.xpath('.//a[contains(@href,"/user/")]')[0]
                    reply_author_id=re.search(r'user/(\d+)', tmp_reply_author.xpath('./@href').get()).group(1)
                    reply_author_name=tmp_reply_author.xpath('.//text()[normalize-space()]').get()
                    reply_text="\n".join(item2.xpath('.//div[@class="x1lliihq xjkvuk6 x1iorvi4"]//text()').extract())
                    tmp_reply_id=item2.xpath(".//li[@class='html-li xdj266r xat24cr xexx8yu x4uap5 x18d9i69 xkhd6sd x1rg5ohu x1emribx x1i64zmx']//a/@href").get()
                    #https://www.facebook.com/groups/443709852472133/posts/2998207153689044/?comment_id=2998961160280310&reply_comment_id=2998962086946884&__cft__[0]=AZWv-ZyTAxQycl1EBdPs7BAa-JJadvWon68g1Tw6rxDbBBJxCvN-7dHIircS1tKHLNzV6BHW54_Kx0-npUMUlmfAsk_Q3-i2Ao9seI6zEMLo73PbDKmkRlIrwrSXrsImVAeIn8kRdgflEvvsPH-XkdCnPiKNa9QYXwOaK1PBTWMm3Q&__tn__=R]-R
                    reply_id=re.search(r'reply_comment_id=(\d+)', tmp_reply_id).group(1)
                    
                    yield({
                        'comment_id':comment_id,
                        'reply_id':reply_id,
                        'author_id':reply_author_id,
                        'message':reply_text,
                        't_timestamp':None
                        })
                    
                    yield({
                        'author_id':reply_author_id,
                        'author_name':reply_author_name,
                    })

            
