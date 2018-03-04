# -*- coding: utf-8 -*-
from collections import Counter
import twitter_data_input as Twi_input
import MeCab
import sqlite3
import math

"""言語処理関係"""



def get_tf(user,doc,db):
	#---- 形態素解析 ----#	
	tagger = MeCab.Tagger("-d /usr/local/lib/mecab/dic/mecab-ipadic-neologd")
	node = tagger.parseToNode(doc.encode('utf-8'))
	noun = [] #名詞リスト

	dbName = "./DB/topicDB/{0}".format(db)
	connect = sqlite3.connect(dbName)
	cur = connect.cursor()

	while node:
		s = node.feature.split(",")
		if unicode(s[0],"utf-8") == u"名詞" and unicode(s[6],"utf-8") != u"*" and s[6] != "ReTweet":
			try:
				a = unicode(node.surface,"utf-8")
				noun.append(unicode(node.surface,"utf-8"))
			except UnicodeDecodeError:
				#print node.surface
				pass
			
			#print s[6]
		
		node = node.next
	#all words
	length = len(noun)
	#same words
	count = Counter(noun)
	#cal tf
	for key in count:
		tf = 1.0*count[key] / length
		sql = "INSERT INTO tf VALUES(?,?,?)"
		cur.execute(sql,(user,key,tf))
		
	connect.commit()
	connect.close()


"""cal df"""
def get_df(db):
	sql = """CREATE TABLE df AS SELECT noun, COUNT(noun) as df FROM tf GROUP BY noun"""
	connect = sqlite3.connect("./DB/topicDB/{0}".format(db))
	cur = connect.cursor()
	cur.execute(sql)

	connect.commit()
	connect.close()


"""cal tf-idf"""
def get_all_idf(normalize_db,user_db):
	user_db = "./DB/topicDB/{0}".format(user_db)
	normalize_db = "./DB/{0}".format(normalize_db)

	def __get_normalize_df(normalize_db):#通常ツイートDF取得 return dict
		connect = sqlite3.connect(normalize_db)
		cur = connect.cursor()

		sql = "SELECT * FROM df"
		cur.execute(sql)

		df_dict = {}

		for noun,count in cur.fetchall():
			df_dict[noun] = count
		connect.commit()
		connect.close()
		return df_dict
	def __get_user_df(user_db):#ユーザのツイートDF取得 return dict
		connect = sqlite3.connect(user_db)
		cur = connect.cursor()

		sql = "SELECT * FROM df"
		cur.execute(sql)

		df_dict = {}

		for noun,count in cur.fetchall():
			df_dict[noun] = count
		connect.commit()
		connect.close()
		return df_dict
	def __append_dict(dict1,dict2):#DF辞書取得 return dict
		nouns = dict2.keys()
		for noun in nouns:
			if dict1.has_key(noun):
				dict1[noun] = dict1[noun] + dict2[noun]
			else:
				dict1[noun] = dict2[noun]
		return dict1 
	def __get_all_dict_num(normalize_db,user_db):#全データ数を数える
		connect = sqlite3.connect(normalize_db)
		cur = connect.cursor()
		all_sum = 0
		sql = "SELECT COUNT(*) FROM docs"
		cur.execute(sql)
		for i in cur.fetchall():
			all_sum = i[0]
			print all_sum
		connect.commit()
		connect.close()

		all_sum = all_sum + 200

		return all_sum
	def __get_idf(user_db,df_list,docs_number,limit_user):#tfidf演算 return dict
		connect = sqlite3.connect(user_db)
		cur = connect.cursor()

		sql = "SELECT * FROM tf"
		cur.execute(sql)
		tf_list = {}
		noun_list = set([])
		user_list = set([])


		for user_id,noun,tf in cur.fetchall():
			try:		#----辞書があるとき-----
				tf_list[user_id][noun] = tf
		#		print "%d|%s|%f" %(user_id,noun,tf_list[user_id][noun])
				noun_list.add(noun)
				user_list.add(user_id)
			except KeyError:	#----辞書がないとき----
				tf_list[user_id] = {}
				tf_list[user_id][noun] = tf
		#		print "%d|%s|%f" %(user_id,noun,tf_list[user_id][noun])
				noun_list.add(noun)
				user_list.add(user_id)
		#	print tf_list.get(user_id,"error")
		#	print tf_list[user_id].get(noun,"error")
		#----------------------

		sql = """CREATE TABLE tfidf(
		user_id text,
		noun varchar(128),
		tfidf double,
		PRIMARY KEY(user_id,noun))"""
		cur.execute(sql)

		tfidf = {}

		for user in user_list:
			for noun in noun_list:
				if tf_list[user].get(noun,"") == "":
					pass
				else:
					try:
						#print limit_user
						#print docs_number
						#print df_list[noun]
						#print (limit_user+docs_number)/df_list[noun]
						tfidf[user][noun] = tf_list[user][noun] * math.log((1.0*limit_user+docs_number)/df_list[noun])
		#				print tfidf[user][noun]
					except KeyError:
						tfidf[user] = {}
						tfidf[user][noun] = tf_list[user][noun] * math.log((1.0*limit_user+docs_number)/df_list[noun])
		#				print tfidf[user][noun]
					sql = """INSERT INTO tfidf(user_id,noun,tfidf)
					VALUES(?,?,?)"""
					cur.execute(sql,(user,noun,tfidf[user][noun]))


		connect.commit()
		connect.close()

		return tfidf


	normalize_df_dict = __get_normalize_df(normalize_db) 
	user_df_dict = __get_user_df(user_db)
	df_dict = __append_dict(normalize_df_dict,user_df_dict)
	all_number = __get_all_dict_num(normalize_db,user_db)
	limit_user = 200
	tfidf_dict = __get_idf(user_db,df_dict,all_number,limit_user)

	return tfidf_dict

"""cal word Vector"""
def get_wordVec(probably,screen_name,tfidf):
	userPer_dict = probably[screen_name]
	user_list = userPer_dict.keys()

	#---- new connection ----#
	database_name = "./DB/topicDB/tf-idf_{0}.db".format(screen_name)
	connect = sqlite3.connect(database_name)
	cur = connect.cursor()

	sql = """SELECT DISTINCT noun FROM tfidf"""
	cur.execute(sql)
	noun_list = []
	for noun in cur.fetchall():
		noun_list.append(noun[0])

	#---- make table ----#
	sql = """CREATE TABLE wordVec(noun text,weight double,PRIMARY KEY(noun))"""
	cur.execute(sql)

	#---- main ----#
	wordVec = {}
	for user in user_list:
		if user == screen_name:
			userPer_dict[screen_name] = 0
		for noun in noun_list:
			try:
				#print noun
				#print user
				a = tfidf[user][noun]
				try:
					wordVec[noun] = wordVec[noun] + userPer_dict[user] * 5 * tfidf[user][noun]
				except KeyError:
					wordVec[noun] = userPer_dict[user] * 5 * tfidf[user][noun]
			except KeyError:
				pass

	#---- write DB ----#
	for noun in noun_list:
		sql = """INSERT INTO wordVec(noun,weight) VALUES(?,?)"""
		cur.execute(sql,(noun,wordVec[noun]))

	#---- close DB ----#
	connect.commit()
	connect.close()#言葉の重みの算出

def get_weight(user_db):
	user_db = "./DB/topicDB/{0}".format(user_db)
	connect = sqlite3.connect(user_db)
	cur = connect.cursor()

	sql = """
	CREATE TABLE wordVec as
	SELECT 	noun,
			SUM(tfidf) as weight
	FROM tfidf
	GROUP BY noun"""

	cur.execute(sql)

	connect.commit()
	connect.close()

"""get topics"""
def get_topics(user_db):
	user_db = "./DB/topicDB/{0}".format(user_db)
	connect = sqlite3.connect(user_db)
	cur = connect.cursor()

	sql = """SELECT * FROM wordVec ORDER BY weight DESC limit 100"""
	cur.execute(sql)

	words_weight = []

	for noun,weight in cur.fetchall():
		words_weight.append((noun,weight))

	return words_weight








