# -*- coding: utf-8 -*-
import twitter_data_output as Twi_output
import twitter_data_input as Twi_input
import language_processing as LP

"""
全体の興味のあるトピック抽出
"""


"""RWR含むユーザの分析"""
def get_target_user_topic(screen_name):
	db = "exist_{0}.db".format(screen_name)
	probably = Twi_output.output_trust(db,screen_name)
	users = probably[screen_name].keys()

	""" get tweets"""
	tweets = Twi_output.output_tweet(users)
	users = tweets.keys()
	docs = {}
	for i in users:
		docs[i] = u""
		for j in tweets[i]:
			docs[i] = docs[i] + j 
	
	db = "tf-idf_{0}.db".format(screen_name)
	normalize_db = "normalize_text.db"


	"""cal tf"""
	Twi_input.input_maketable_tf(db)
	
	for user in users:
		LP.get_tf(user,docs[user],db)


	"""cal df"""
	LP.get_df(db)

	"""cal tf-idf"""
	tfidf = LP.get_all_idf(normalize_db,db)

	"""cal wordVec"""
	LP.get_wordVec(probably,screen_name,tfidf)

"""RWR含まないユーザの分析"""
def get_user_topic(screen_name):
	"""followを取得"""
	users = Twi_output.output_follow(screen_name)

	""" get tweets"""
	tweets = Twi_output.output_tweet(users)
	docs = {}

	users = tweets.keys()

	for i in users:
		docs[i] = u""
		for j in tweets[i]:
			docs[i] = docs[i] + j

	db = "tf-idf_{0}.db".format(screen_name)
	normalize_db = "normalize_text.db"

	"""cal tf"""
	Twi_input.input_maketable_tf(db)
	
	for user in users:
		LP.get_tf(user,docs[user],db)


	"""cal df"""
	LP.get_df(db)

	"""cal tf-idf"""
	tfidf = LP.get_all_idf(normalize_db,db)

	"""cal weight"""
	LP.get_weight(db)


"""あるユーザとその付近のユーザ分析"""
def get_topic(screen_name):
	db = "exist_{0}.db".format(screen_name)
	probably = Twi_output.output_trust(db,screen_name)
	users = probably[screen_name].keys()

	get_target_user_topic(screen_name)

	for user in users:
		get_user_topic(user)

"""weight取得用"""
def get_weight(screen_name):
	db = "exist_{0}.db".format(screen_name)
	probably = Twi_output.output_trust(db,screen_name)
	users = probably[screen_name].keys()

	""" get tweets"""
	tweets = Twi_output.output_tweet(users)
	users = tweets.keys()

	for user in users:
		#print user
		db = "tf-idf_{0}.db".format(user)
		LP.get_weight(db)

"""全体のトピックワード抽出"""
def get_topic_words(screen_name):
	db = "exist_{0}.db".format(screen_name)
	probably = Twi_output.output_trust(db,screen_name)
	users = probably[screen_name].keys()

	""" get tweets"""
	tweets = Twi_output.output_tweet(users)
	users = tweets.keys()

	all_weight = []
	for user in users:
		#print user
		db = "tf-idf_{0}.db".format(user)
		all_weight.append(LP.get_topics(db))

	point = 101
	rank = {}
	print len(all_weight)
	print len(all_weight[0])
	for i in range(len(all_weight[0])):
		point = point - 1
		for j in range(len(all_weight)):
			if rank.has_key(all_weight[j][i][0]):
				pass
			else:
				rank[all_weight[j][i][0]] = 0
			rank[all_weight[j][i][0]] = rank[all_weight[j][i][0]] + point

	#print rank
	Twi_input.input_topic_words(rank)


if __name__ == "__main__":
	screen_name = u"Teara_exe"
	#get_target_user_topic(screen_name)
	#get_topic(screen_name)
	#get_weight(screen_name)
	get_topic_words(screen_name)