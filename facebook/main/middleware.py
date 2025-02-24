import datetime
import json
import os
import random
import re
import sqlite3
import traceback
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import StaleElementReferenceException, ElementNotInteractableException
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager
from scrapy.http import HtmlResponse
import time

class SeleniumMiddleware:
    """
    Scrapy Middleware that uses Selenium to fetch dynamically loaded pages.
    """

    def __init__(self, settings):
        chrome_options = Options()
        # options.add_argument("--headless=new")  # 可選，無頭模式
        chrome_options.add_argument("--log-level=3")
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-popup-blocking")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")  # 防止被 Facebook 偵測
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36")

        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

        # **移除 Selenium WebDriver 痕跡**
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        # **刪除 Selenium 內建變數**
        self.driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
                delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
                delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;
            """
        })

        self.conn = sqlite3.connect(settings.get('SQLITE_DB_NAME'))
        self.cursor = self.conn.cursor()
        
        
        
        self.cursor.execute("SELECT post_id,t_timestamp FROM fb_posts where author_id is null")
        self.sraped_post_data = list(self.cursor.fetchall())
        self.sraped_post_id_list = [item[0] for item in self.sraped_post_data]
        
        self.post_data = []
        self.post_id_list = []

        self.email = settings.get('FB_EMAIL')
        self.password = settings.get('FB_PASSWORD')
        self.min_post_timestamp =datetime.datetime.now()
        self.__login()
    
    @classmethod
    def from_crawler(cls, crawler):
        """Create middleware instance with settings access."""
        return cls(crawler.settings)
    
    def __login(self):
        """Logs into Facebook if mode is 'loggedin'."""
        self.driver.get('https://www.facebook.com/login')
        time.sleep(random.uniform(2, 4))  # 模擬人類等待

        email_input = self.driver.find_element(By.NAME, 'email')
        password_input = self.driver.find_element(By.NAME, 'pass')

        # **模擬人類鍵盤輸入**
        for char in self.email:
            email_input.send_keys(char)
            time.sleep(random.uniform(0.1, 0.3))

        for char in self.password:
            password_input.send_keys(char)
            time.sleep(random.uniform(0.1, 0.3))

        password_input.send_keys(Keys.RETURN)

        # **等待登入完成**
        try:
            WebDriverWait(self.driver, 10).until(EC.url_contains("facebook.com"))
            print("Login successful!")
        except:
            os.system("pause")
            raise Exception("Login failed. Check your credentials.")
            

    def process_request(self, request, spider):
        """
        Fetches the URL using Selenium and returns an HtmlResponse.
        """
        self.driver.get(request.url)
        page_type = request.meta.get("page_type")
        print(f"page_type: {page_type}")
        if page_type == "group":
            return self.scroll_and_collect_posts(request)

        elif page_type=="post":
            return self.get_posts_and_expand(request)

        elif page_type=="comment":
            pass
        elif page_type=="reply":
            pass   
        else:
            time.sleep(5)
           



    def get_all_posts(self):
        """獲取目前已載入的 Facebook 貼文"""
        return self.driver.find_elements(By.XPATH, '//div[@role="feed"]//div[@class="x1yztbdb x1n2onr6 xh8yej3 x1ja2u2z"]//div[@class="html-div xdj266r x11i5rnm xat24cr x1mh8g0r xexx8yu x4uap5 x18d9i69 xkhd6sd x1iyjqo2"][1]//a[@role="link"  and not(contains(@href, "user")) and  ( (contains(@href, "/posts/") or contains(@href, "__cft__") ) ) ]')

    def get_load_posts(self):
        print("get_load_posts")
        """獲取目前已載入完整的 Facebook 貼文"""
        return self.driver.find_elements(By.XPATH, '//div[@role="feed"]//div[@class="x1yztbdb x1n2onr6 xh8yej3 x1ja2u2z"]//div[@class="html-div xdj266r x11i5rnm xat24cr x1mh8g0r xexx8yu x4uap5 x18d9i69 xkhd6sd x1iyjqo2"][1]//a[@role="link" and starts-with(@href, "https://www.facebook.com/groups/") and contains(@href, "/posts/")]')

    def get_unload_post_url(self):
        print("get_unload_post_url")
        """獲取目前未載入完整的 Facebook 貼文"""
        tmp_result=self.driver.find_elements(By.XPATH, '//div[@role="feed"] //div[@class="x1yztbdb x1n2onr6 xh8yej3 x1ja2u2z"] ')
        tmp2=[]
        tmp3=[]
        for item in tmp_result:
            try:
                finds=item.find_elements(By.XPATH, './/div[@class="html-div xdj266r x11i5rnm xat24cr x1mh8g0r xexx8yu x4uap5 x18d9i69 xkhd6sd x1q0g3np"]')
                if len(finds)>0:
                    tmp2.append(finds[0])
                
            except:
                os.system("pause")
        for item in tmp2:
            finds=item.find_elements(By.XPATH, './/a[position()=last()]')
            if len(finds)>0:
                tmp3.append(finds[0])

        return [item for item in tmp3 if not re.search(r'posts/(\d+)', item.get_attribute("href")) ]

    def get_unload_post_url_on_post_page(self):
        print("get_unload_post_url_on_post_page")
        """獲取Facebook 貼文的時間"""
        tmp_result=self.driver.find_elements(By.XPATH, '//div[@class="x1yztbdb x1n2onr6 xh8yej3 x1ja2u2z"] ')
        tmp2=[]
        tmp3=[]

        for item in tmp_result:
            try:
                finds=item.find_elements(By.XPATH, './/div[@class="html-div xdj266r x11i5rnm xat24cr x1mh8g0r xexx8yu x4uap5 x18d9i69 xkhd6sd x1q0g3np"]')
                if len(finds)>0:
                    tmp2.append(finds[0])
                
            except:
                os.system("pause")

        for item in tmp2:
            finds=item.find_elements(By.XPATH, './/a[position()=last()]')
            if len(finds)>0:
                tmp3.append(finds[0])
        
     
        
        self.scroll_to_top()
        return [item for item in tmp3 if re.search(r'posts/(\d+)', item.get_attribute("href")) ]



    def get_timestamp(self):
        return self.driver.find_elements(By.XPATH, '//div[@class="__fb-dark-mode"]')
    
    def get_timestamp_from_post_id(self, post_id):
        """從 self.sraped_post_data 獲取指定貼文的時間戳"""
        for post in self.sraped_post_data:
            if post[0]== post_id:
                return post[1]
        raise ValueError(f"Post ID {post_id} not found in self.sraped_post_data.")
        


    def hover_timestamp_and_parse(self, item):
        print("hover_timestamp_and_parse")
        try:
            # self.driver.execute_script("""
            #     var event = new MouseEvent('mouseover', {
            #         'view': window,
            #         'bubbles': true,
            #         'cancelable': true
            #     });
            #     arguments[0].dispatchEvent(event);
            # """, item)
            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", item)
            time.sleep(0.5)  # 等待滾動完成

            actions = ActionChains(self.driver)
            actions.move_to_element(item).perform()
            time.sleep(1)
            # Wait until timestamp appears
            timestamps = WebDriverWait(self.driver, 5).until(
                EC.presence_of_all_elements_located((By.XPATH, '//div[@class="__fb-dark-mode"]'))
            )
            
            tmp_timestamp=timestamps[-1].text.strip() if timestamps else None

            # remove timestamp element
            for timestamp in timestamps:
                self.driver.execute_script("arguments[0].remove();", timestamp)
            
            return tmp_timestamp
        
        
        #* if not interactable*
        except ElementNotInteractableException:
            print("⚠️ ElementNotInteractableException")
            return None
            
        
        except Exception as e:
            print(e)
            traceback.print_exc()
            
            return None

    def get_ok(self):
        #//span[contains(text(), "OK") ]
        return self.driver.find_elements(By.XPATH, '//span[contains(text(), "OK")')

    def hover_timestamp_and_parse_for_post(self, item):
        print("hover_timestamp_and_parse")
        try:
          
            self.scroll_to_top()
  
            actions = ActionChains(self.driver)
            actions.move_to_element(item).perform()
            time.sleep(1)
            # Wait until timestamp appears
            timestamps = WebDriverWait(self.driver, 5).until(
                EC.presence_of_all_elements_located((By.XPATH, '//div[@class="__fb-dark-mode"]'))
            )
            
            tmp_timestamp=timestamps[-1].text.strip() if timestamps else None

            # remove timestamp element
            for timestamp in timestamps:
                self.driver.execute_script("arguments[0].remove();", timestamp)
            
            return tmp_timestamp
        
        
        #* if not interactable*
        except ElementNotInteractableException:
            print("⚠️ ElementNotInteractableException")
            return None
            
        
        except Exception as e:
            print(e)
            traceback.print_exc()
            if self.get_ok():
                self.get_ok()[0].click()
                time.sleep(10)
                
            return None

    def update_scraped_item(self, post_id, tmp_timestamp):
        self.post_id_list.append(post_id)
        self.post_data.append({"post_id": post_id,"t_timestamp":tmp_timestamp})
        
        self.sraped_post_id_list.append(post_id)
        self.sraped_post_data.append((post_id,tmp_timestamp))

    def scroll_page(self):
        """Scrolls the page to load more posts."""
        # Random scrolling distance
        scroll_distance = random.randint(300, 800)
        self.driver.execute_script(f"window.scrollBy(0, {scroll_distance});")

        # Random wait time
        time.sleep(random.uniform(1, 3))


    def scroll_to_top(self):
        print("scroll_to_top")
        scroll_distance = -300  # 每次滾動 300px
        while True:
            prev_position = self.driver.execute_script("return window.scrollY;")  # 取得當前滾動位置
            self.driver.execute_script(f"window.scrollBy(0, {scroll_distance});")  # 向上滾動
            time.sleep(0.5)  # 等待動畫效果
            new_position = self.driver.execute_script("return window.scrollY;")  # 取得滾動後位置
            if new_position == 0 or new_position == prev_position:  # 如果滾動到頂部或無法再滾動
                break


    def scroll_and_collect_posts(self, request):
        print("scroll_and_collect_posts")
        min_posts = 10  # Target number of posts

        while True:
            if self.min_post_timestamp < datetime.datetime.now() - datetime.timedelta(days=366):
                print("🛑 Stopping scrolling...")
                json_body = json.dumps( { "post_data":[ ] }  , ensure_ascii=False)
                break
            else:
                try:
                    
                    posts = self.get_unload_post_url()

                    while posts:
                        post=posts[0]
                        time_str=self.hover_timestamp_and_parse(post)
                        post_url = posts[0].get_attribute("href")
                        post_id = re.search(r'posts/(\d+)', post_url).group(1) if re.search(r'posts/(\d+)', post_url) else None

                        if post_id and post_id not in self.sraped_post_id_list and time_str:
                            print(f"Processing post: {post_url},timestamp:{time_str}")
                            try:
                                tmp_timestamp = datetime.datetime.strptime(time_str, "%A, %B %d, %Y at %I:%M %p")
                                self.cursor.execute(
                                    "INSERT OR IGNORE INTO fb_posts (group_id, post_id, t_timestamp,upd_time) VALUES (?, ?, ?, DATETIME('now'))",
                                    (request.meta.get("group_id"),post_id,tmp_timestamp)
                                )
                                self.conn.commit()


                                self.update_scraped_item(post_id, tmp_timestamp)
                            
                            except Exception as e:
                                print(e)
                                
                                
                            
   

                        loaded_posts = self.get_load_posts()


                        for post in loaded_posts[:-2]:                       
                            try:
                                tmp_timestamp=None
                                post_url = post.get_attribute("href")
                                post_id = re.search(r'posts/(\d+)', post_url).group(1) if re.search(r'posts/(\d+)', post_url) else None
                                print(f"post_id:{post_id}")
                                if post_id and post_id not in self.sraped_post_id_list:
                                    time_str=self.hover_timestamp_and_parse(post)
            
                                    if time_str:
                                        print(f"Processing post: {post_url},timestamp:{time_str}")
                                        tmp_timestamp = datetime.datetime.strptime(time_str, "%A, %B %d, %Y at %I:%M %p")
                                        
                                        self.update_scraped_item(post_id, tmp_timestamp)
                                    
                                    else:
                                        print("⚠️ Failed to get timestamp, skipping...")
                                        break

                                elif post_id and post_id in self.post_id_list:
                                    tmp_timestamp = self.get_timestamp_from_post_id(post_id)
                                
                                
                                if tmp_timestamp and tmp_timestamp<self.min_post_timestamp:
                                    self.min_post_timestamp=tmp_timestamp


                            except StaleElementReferenceException:
                                print("⚠️ Post element went stale, skipping...")
                            
                            print(f"Removed post: {post_id}")   
                            try:
                                parent_div = post.find_element(By.XPATH, "./ancestor::div[@class='x1yztbdb x1n2onr6 xh8yej3 x1ja2u2z']")
                                # 使用 JavaScript 移除該元素
                                if parent_div:
                                    self.driver.execute_script("arguments[0].remove();", parent_div)
                                time.sleep(0.5)
                            except:
                                pass
                            

                        posts = self.get_unload_post_url()
                    
                    else:
                        print("No more posts to load.")
                        

                    
                    self.scroll_page()
                    print(f"🔄 Scrolling... Collected posts: {len(self.post_id_list)}")
                    

                except StaleElementReferenceException:
                    print("⚠️ Entire page updated, refetching posts...")
                    continue  # Retry fetching posts after scrolling



        json_body = json.dumps( { "post_data":[ {"post_id":item.get("post_id"),"t_timestamp":item.get("t_timestamp").isoformat() } for item in self.post_data] }  , ensure_ascii=False)
        self.post_data.clear()

        return HtmlResponse(url=request.url, body=json_body, encoding="utf-8", request=request)

    def get_min_post_timestamp(self):
        """從self.post_data獲取最早貼文的時間戳"""
        return min(self.post_data, key=lambda x: x["t_timestamp"])["t_timestamp"]


    def get_view_more_reply(self):
        return self.driver.find_elements(By.XPATH, '//span[contains(text(), "View") and contains(text(), "repl")]')
    
    def get_view_more_comments(self):
        return self.driver.find_elements(By.XPATH, '//span[contains(text(), "View") and contains(text(), "comment")]')
    
    def get_see_more(self):
        return self.driver.find_elements(By.XPATH, '//div[@role="button" and contains(text(), "See more") ]')

    def expand_replies(self):
        tmp=self.get_view_more_reply()
        while tmp:
            for item in tmp:
                try:
                    item.click()
                    time.sleep(0.1)
                except:
                    pass          
            time.sleep(2)
            tmp=self.get_view_more_reply()
    
    def expand_comments(self):
        tmp=self.get_view_more_comments()
        while tmp:
            for item in tmp:
                try:
                    item.click()
                    time.sleep(0.1)
                except:
                    pass
            time.sleep(2)
            tmp=self.get_view_more_comments()

    def expand_see_more(self):
        tmp=self.get_see_more()
        while tmp:
            for item in tmp:
                try:
                    item.click()
                    time.sleep(0.1)
                except: 
                    pass
            time.sleep(2)
            tmp=self.get_see_more()



    def get_posts_and_expand(self, request):
        """將未載入完整的 Facebook 貼文載入"""

        menu_list=self.driver.find_elements(By.XPATH, '//div[@class="x6s0dn4 x78zum5 xdj266r x11i5rnm xat24cr x1mh8g0r xe0p6wg"]//*[@aria-haspopup="menu"]')
        if menu_list:
            menu_list[0].click()
            # 等待 "All comments" 出現
            wait = WebDriverWait(self.driver, 10)
            all_comments_button = wait.until(
                EC.presence_of_element_located((By.XPATH, "//span[text()='All comments']"))
            )
            # 點擊該按鈕
            all_comments_button.click()
        
        time.sleep(2)

        self.expand_comments()
        self.expand_replies() 
        self.expand_see_more()
        
        posts = self.get_unload_post_url_on_post_page()
        t_timestamp=None
        if posts:
            time_str=self.hover_timestamp_and_parse_for_post(posts[0])
            print(f"Processing post: {posts[0].get_attribute('href')},timestamp:{time_str}")
            try:
                t_timestamp = datetime.datetime.strptime(time_str, "%A, %B %d, %Y at %I:%M %p")
            except:
                os.system("pause")
            
        else:
            os.system("pause")
            raise ValueError("No timestamp found.")
        #//div[@class="x1yztbdb x1n2onr6 xh8yej3 x1ja2u2z"]//div[@class="html-div xdj266r x11i5rnm xat24cr x1mh8g0r xexx8yu x4uap5 x18d9i69 xkhd6sd x1q0g3np"]//a[position()=last()]
        
        # 可以透過 request.meta 傳遞額外的資訊
        request.meta['t_timestamp'] = t_timestamp
        
        return HtmlResponse(url=request.url, body=self.driver.page_source, encoding="utf-8")


    def __del__(self):
        """關閉 Selenium 瀏覽器"""
        self.driver.quit()
