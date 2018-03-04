# -*- coding: utf-8 -*-
import sqlite3
import json

def input_screenName(ids,screen_name):
	dbName = "./DB/screen_name.db"
	connect = sqlite3.connect(dbName)
	cur = connect.cursor()

	sql = """INSERT INTO screen_name VALUES(?,?)"""
	cur.execute(sql,(ids,screen_name))

	connect.commit()
	connect.close()

"""ネットワークの書き込み"""
def input_network(users,dbName):
	dbName = "./DB/{0}".format(dbName)
	connect = sqlite3.connect(dbName)
	cur = connect.cursor()

	sql = """CREATE TABLE network(user text,followee text)"""
	cur.execute(sql)

	user = users.keys()
	for i in user:
		for j in users[i]:
			sql = """INSERT INTO network VALUES(?,?)"""
			cur.execute(sql,(i,j))
	connect.commit()
	connect.close()

def input_network_with_drop(users,dbName):
	dbName = "./DB/{0}".format(dbName)
	connect = sqlite3.connect(dbName)
	cur = connect.cursor()

	sql = """DROP TABLE network"""
	cur.execute(sql)

	sql = """CREATE TABLE network(user text,followee text)"""
	cur.execute(sql)

	user = users.keys()
	for i in user:
		for j in users[i]:
			sql = """INSERT INTO network VALUES(?,?)"""
			cur.execute(sql,(i,j))
	connect.commit()
	connect.close()

"""ネットワーク書き出し(json)"""
def input_network_buff_json(users,filename):
	json_name = "{0}".format(filename)
	with open(json_name,"w") as f:
		json.dump(users,f,sort_keys = True, indent = 4)


"""F------存在数の書き込み"""
def F_input_exist_count(per,dbName,start_user):
	dbName = "./DB/{0}".format(dbName)
	connect = sqlite3.connect(dbName)
	cur = connect.cursor()

	sql = """CREATE TABLE exist_count(start_user text,user text PRIMARY KEY,count real)"""
	cur.execute(sql)

	user = per.keys()
	for i in user:
		sql = """INSERT INTO exist_count VALUES(?,?,?)"""
		cur.execute(sql,(start_user,i,per[i]))
	connect.commit()
	connect.close()


"""ツイートDB制作"""
def input_make_tweetDB():
	dbName = "./DB/tweet.db"
	connect = sqlite3.connect(dbName)
	cur = connect.cursor()

	sql = """CREATE TABLE tweet(user text, tweet text)"""
	cur.execute(sql)

	connect.commit()
	connect.close()




"""ツイートインプット"""
def input_tweet(user,tweet):
	dbName = "./DB/tweet.db"
	connect = sqlite3.connect(dbName)
	cur = connect.cursor()

	sql = """INSERT INTO tweet VALUES(?,?)"""

	for i in tweet:
		cur.execute(sql,(user,i))

	connect.commit()
	connect.close()


"""make table tf"""
def input_maketable_tf(db):
	dbName = "./DB/{0}".format(db)
	connect = sqlite3.connect(dbName)
	cur = connect.cursor()

	sql = "CREATE TABLE tf(user text,noun text,tf real)"
	cur.execute(sql)

	connect.commit()
	connect.close()


"""insert tf"""
def input_tf(db,user,noun,tf):
	dbName = "./DB/{0}".format(db)
	connect = sqlite3.connect(dbName)
	cur = connect.cursor()

	sql = "INSERT INTO tf VALUES(?,?,?)"
	cur.execute(sql,(user,unicode(noun,"utf-8"),tf))

	connect.commit()
	connect.close()

"""insert topic words"""
def input_topic_words(rank):

	db = "./DB/topic_words.db"
	connect = sqlite3.connect(db)
	cur = connect.cursor()


	sql = """CREATE TABLE topic_words(noun text, score real)"""
	cur.execute(sql)

	noun = rank.keys()

	sql = """INSERT INTO topic_words VALUES(?,?)"""
	for i in noun:
		if i != " " and i != "\n" and i != "":
			cur.execute(sql,(i,rank[i]))

	connect.commit()
	connect.close()


