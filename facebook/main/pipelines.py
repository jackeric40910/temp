# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import os.path
import pymssql
import decimal
import datetime
import re
import sqlite3



    
class SQLitePipeline:
    """
    Scrapy pipeline to store scraped data in SQLite database.
    """
    
    def open_spider(self, spider):
        """Initialize SQLite database and create tables if they don't exist."""
        self.conn = sqlite3.connect('data.db')
        self.cursor = self.conn.cursor()
        
        # Create tables if they don't exist
       
        self.post_list = []
        self.comment_list = []
        self.reply_list = []
        self.author_list = []
    
    def process_item(self, items, spider):
        """Process and store each scraped item in SQLite database."""
        group_id=items.get('group_id')
        if group_id and  items.get('post_data'):
           
            for post in items['post_data']:
                # CREATE TABLE "fb_posts" (
                # "group_id"	TEXT NOT NULL,
                # "post_id"	TEXT,
                # "author_id"	TEXT,
                # "message"	TEXT,
                # "media_url"	TEXT,
                # "link_url"	TEXT,
                # "likes_count"	INTEGER DEFAULT 0,
                # "comments_count"	INTEGER DEFAULT 0,
                # "shares_count"	INTEGER DEFAULT 0,
                # "t_timestamp"	DATETIME DEFAULT CURRENT_TIMESTAMP,
                # PRIMARY KEY("post_id")
                # )
                self.post_list.append( (
                        group_id,
                        post.get('post_id'),
                        post.get('author_id'),
                        post.get('message'),
                        post.get('media_url'),
                        post.get('link_url'),
                        post.get('likes_count'),
                        post.get('comments_count'),
                        post.get('shares_count'),
                        post.get('t_timestamp')
                        )
                    )
      
        if items.get('post_id') and items.get('comment_id'):
            #    CREATE TABLE "fb_comments" (
            #         "post_id"	TEXT NOT NULL,
            #         "comment_id"	TEXT,
            #         "author_id"	TEXT NOT NULL,
            #         "message"	TEXT,
            #         "t_timestamp"	DATETIME DEFAULT CURRENT_TIMESTAMP,
            #         "upd_time"	DATETIME DEFAULT CURRENT_TIMESTAMP,
            #         PRIMARY KEY("comment_id")
            #     );
            self.comment_list.append( (
                    items.get('post_id'),
                    items.get('comment_id'),
                    items.get('author_id'),
                    items.get('message'),
                    items.get('t_timestamp')
                    )
                )
            
        if items.get('comment_id') and items.get('reply_id'):
            # CREATE TABLE "fb_reply" (
            #     "comment_id"	TEXT NOT NULL,
            #     "reply_id"	TEXT,
            #     "author_id"	TEXT NOT NULL,
            #     "message"	TEXT,
            #     "t_timestamp"	DATETIME DEFAULT CURRENT_TIMESTAMP,
            #     "upd_time"	DATETIME DEFAULT CURRENT_TIMESTAMP,
            #     PRIMARY KEY("reply_id")
            # )
            self.reply_list.append( (
                    items.get('comment_id'),
                    items.get('reply_id'),
                    items.get('author_id'),
                    items.get('message'),
                    items.get('t_timestamp')
                    )
                )
            
        if items.get('author_id') and items.get('author_name'):
            # CREATE TABLE "fb_authors" (
            #     "author_id"	TEXT NOT NULL,
            #     "author_name"	TEXT NOT NULL,
            #     PRIMARY KEY("author_id")
            # )
            self.author_list.append( (
                    items.get('author_id'),
                    items.get('author_name')
                    )
                )
        
       
        self.bulk_insert()
    
    def bulk_insert(self,amount=10):
        """Bulk insert data into SQLite tables for performance optimization."""
        try:
            if len(self.post_list) >= amount:
                self.cursor.executemany(
                    "INSERT OR REPLACE INTO fb_posts (group_id, post_id, author_id, message, media_url, link_url, likes_count, comments_count, shares_count, t_timestamp,upd_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DATETIME('now'))",
                    self.post_list
                )
                self.conn.commit()
                self.post_list.clear()
                
            if len(self.comment_list) >= amount:
                # CREATE TABLE "fb_comments" (
                #     "post_id"	TEXT NOT NULL,
                #     "comment_id"	TEXT,
                #     "author_id"	TEXT NOT NULL,
                #     "message"	TEXT,
                #     "t_timestamp"	DATETIME DEFAULT CURRENT_TIMESTAMP,
                #     PRIMARY KEY("comment_id")
                # )
                self.cursor.executemany(
                    "INSERT OR REPLACE INTO fb_comments (post_id, comment_id, author_id, message, t_timestamp, upd_time) VALUES (?, ?, ?, ?, ?,DATETIME('now'))",
                    self.comment_list

                )
                self.conn.commit()
                self.comment_list.clear()
                
            if len(self.reply_list) >= amount:
                # CREATE TABLE "fb_reply" (
                #     "comment_id"	TEXT NOT NULL,
                #     "reply_id"	TEXT,
                #     "author_id"	TEXT NOT NULL,
                #     "message"	TEXT,
                #     "t_timestamp"	DATETIME DEFAULT CURRENT_TIMESTAMP,
                #     PRIMARY KEY("reply_id")
                # )
                self.cursor.executemany(
                    "INSERT OR REPLACE INTO fb_reply (comment_id, reply_id, author_id, message, t_timestamp, upd_time) VALUES (?, ?, ?, ?, ?,DATETIME('now'))",
                    self.reply_list
                
                )
                self.conn.commit()
                self.reply_list.clear()

            if len(self.author_list) >= amount:
                # CREATE TABLE "fb_authors" (
                #     "author_id"	TEXT NOT NULL,
                #     "author_name"	TEXT NOT NULL,
                #     PRIMARY KEY("author_id")
                # )
                self.cursor.executemany(
                    'INSERT OR REPLACE INTO fb_authors (author_id, author_name) VALUES (?, ?)',
                    self.author_list
                )
                self.conn.commit()
                self.author_list.clear()
                
        except Exception as e:
            print(f"SQLite Insert Error: {e}")
    
    def close_spider(self, spider):
        """Commit any remaining data and close the SQLite connection."""
        self.bulk_insert()  # Insert any remaining data
        self.conn.close()
