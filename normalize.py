# -*- coding: utf-8 -*-
import sqlite3
import MeCab
import normalize_tweet
import math

"""一般語の削除用DB作成"""

def split_tweets(db,max_number):
	connect = sqlite3.connect(db)
	cur = connect.cursor()

	def __split_docs(cur,get_max_data):	#CREATE TABLE docs(doc_num integer,docs text) ツイートを200ごとに分割
		sql = "CREATE TABLE docs(doc_num integer,docs text)"
		cur.execute(sql)
		i = 1
		docs_number = 1
		docs = []
		print "make table..."

		while i < max_number:	#10000件ずつ取得
			sql = "SELECT * FROM normal_tweets WHERE id >= ? AND id < ? "
			cur.execute(sql,(i,i+get_max_data))

			j = 1 
			s = ""
			for ids, tweet in cur.fetchall():
				if j <= 200:	#200件ずつ一つにまとめる
					s = s + tweet
					j = j + 1
				if j == 200:	
					j = 1
					s = normalize_tweet.normalize_text(s)
					#print s
					docs.append(s)
					s = ""

			i = i + get_max_data	#次回開始ポイント設定

			for doc in docs:
				print doc
				sql = "INSERT INTO docs VALUES(?,?)"
				cur.execute(sql,(docs_number,doc))
				docs_number = docs_number + 1
		connect.commit()
		connect.close()

	__split_docs(cur,max_number)
	

def get_df(db):
	connect = sqlite3.connect("./DB/{0}".format(db))
	cur = connect.cursor()

	#---- make table ----#
	sql = u"CREATE TABLE nouns(doc_num integer,noun text)"
	cur.execute(sql)

	sql = u"SELECT * FROM docs"
	cur.execute(sql)

	for ids, tweets in cur.fetchall():
		def __parse_noun_node(s):#---- 形態素解析(名詞返す) ----#	
			tagger = MeCab.Tagger("-Ochasen")
			node = tagger.parseToNode(s.encode('utf-8'))
			noun = [] #名詞リスト
			while node:
				if unicode(node.feature.split(",")[0],"utf-8") == u"名詞" and unicode(node.feature.split(",")[6],"utf-8") != u"*":
					noun.append(s[6])
				node = node.next
			return noun
		nouns = list(set(__parse_noun_node(tweets)))
		sql = u"INSERT INTO nouns VALUES(?,?)"
		for noun in nouns:
			print type(noun)
			try:
				cur.execute(sql,(ids,noun))
			except UnicodeDecodeError:
				print noun

	sql = u"""
			CREATE TABLE df AS
			SELECT noun AS noun,
				   count(*) AS cnt
			FROM nouns 
			GROUP BY noun"""
	cur.execute(sql)

	connect.commit()
	connect.close()

def get_all_idf(normalize_db,user_db):

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

		connect = sqlite3.connect(user_db)
		cur = connect.cursor()
		sql = "SELECT COUNT(*) FROM docs"
		cur.execute(sql)
		for i in cur.fetchall():
			all_sum = i[0] + all_sum
			print all_sum
		connect.commit()
		connect.close()

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
						tfidf[user][noun] = tf_list[user][noun] * math.log((limit_user+docs_number)/df_list[noun])
		#				print tfidf[user][noun]
					except KeyError:
						tfidf[user] = {}
						tfidf[user][noun] = tf_list[user][noun] * math.log((limit_user+docs_number)/df_list[noun])
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

def getWordVec(database,screen_name,limit_user,alpha,limit_time,tfidf):
	#---- connection ----#
	database_name = "./{0}".format(database)
	connect = sqlite3.connect(database_name)
	cur = connect.cursor()

	#---- get % from db ----#
	sql = """SELECT * FROM p_{0}_{1}h ORDER BY per DESC LIMIT ?""".format(int(alpha*100),limit_time)
	cur.execute(sql,(limit_user,))
	userPer_dict = {}
	for user,per in cur.fetchall():
		userPer_dict[user] = per

	#---- close DB ----#
	connect.commit()
	connect.close()

	#---- new connection ----#
	database_name = "./tfidf_{0}.db".format(screen_name)
	connect = sqlite3.connect(database_name)
	cur = connect.cursor()

	#---- get user & word ----#
	sql = """SELECT DISTINCT user_id FROM tfidf"""
	cur.execute(sql)
	user_list = []
	for user in cur.fetchall():
		user_list.append(user[0])
		#print user[0]
		#print isinstance(user[0],unicode)
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


s = "normalize_text.db"
max_tweets = 750000
s_="tfidf_xnkla_.db"
db = "xnkla_pro.db"
#split_tweets(s,max_tweets)
get_df(s)
#tfidf = get_all_idf(s,s_)
#print tfidf
#getWordVec(db,"xnkla_",200,0.5,48,tfidf)


