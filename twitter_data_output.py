# -*- coding: utf-8 -*-
import sqlite3

"""ネットワークの取り出し 連想配列で返す"""
def output_network(database):
	database = "./DB/{0}".format(database)
	connect = sqlite3.connect(database)
	cur = connect.cursor()

	sql = """SELECT * FROM network"""
	cur.execute(sql)

	users = {}
	for user,follow in cur.fetchall():
		if users.has_key(user):
			users[user].append(follow)
		else:
			users[user] = []
			users[user].append(follow)

	return users


"""ユーザの信頼度取得"""
def output_trust(database,screen_name):
	database = "./DB/{0}".format(database)
	connect = sqlite3.connect(database)
	cur = connect.cursor()

	sql = """SELECT SUM(count) from exist_count"""
	cur.execute(sql)

	counter =0
	for i in cur.fetchall():
		counter = i[0]
		#print counter

	sql = """SELECT user, count FROM exist_count WHERE start_user = ? GROUP BY user ORDER BY count DESC limit 200"""
	cur.execute(sql,(screen_name,))

	probably = {}
	probably[screen_name] = {}
	for user,per in cur.fetchall():
		probably[screen_name][user] =1.0*per/counter
		#print "{0}->{1} probably = {2}".format(screen_name,user,1.0*per/counter)
	return probably


"""緊急 取得済みユーザの取得"""
def output_delete_user():
	db = "./DB/tweet.db"
	connect = sqlite3.connect(db)
	cur = connect.cursor()

	sql = """SELECT DISTINCT user from tweet"""
	cur.execute(sql)

	delete = []
	for user in cur.fetchall():
		delete.append(user[0])

	return delete

"""ユーザのツイートを取得"""
def output_tweet(users):
	db = "./DB/tweet.db"
	connect = sqlite3.connect(db)
	cur = connect.cursor()

	tweets = {}
	sql = """SELECT * FROM tweet WHERE user="""
	flg = True
	for i in users:
		if flg:
			sql = sql + '"'+i+'"'
			flg = False
		else:
			sql = sql + ' or user="{0}"'.format(i)
	#print sql
	cur.execute(sql)

	for user, tweet in cur.fetchall():
		if tweets.has_key(user):
			tweets[user].append(tweet)
		else:
			tweets[user] = []
			tweets[user].append(tweet)

	#print tweets.keys()
	#print tweets[tweets.keys()[0]][0]
	return tweets

	connect.close()

"""ユーザのfollowを取得 注意!!!!"""
def output_follow(screen_name):
	db = "./DB/network_Teara_exe.db"
	connect = sqlite3.connect(db)
	cur = connect.cursor()

	sql = "SELECT * FROM network WHERE user=?"
	cur.execute(sql,(screen_name,))

	followees = []
	for user,followee in cur.fetchall():
		followees.append(followee)

	connect.close()

	return followees


if __name__ == "__main__":
	u = output_tweet([u"Teara_exe",u"xnkla_"])