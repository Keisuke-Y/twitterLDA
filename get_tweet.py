# -*- coding: utf-8 -*-
import twitter_module as TM
import twitter_data_output as Twi_output
import twitter_data_input as Twi_input
import normalize_tweet as Normalize

"""ユーザのフォローしている人のツイート取得"""


def get_user_tweet(network_database):
	network = Twi_output.output_network("./DB/{0}".format(network_database))
	twitter = TM.Twitter()
	url = TM.ApiUrl()

	#Twi_input.input_make_tweetDB()

	"""全ユーザ取り出し"""
	def __get_all_user(network):
		user = network.keys()
		all_user = user

		for i in user:
			all_user = network[i] + all_user

		all_user = list(set(all_user))
		#print len(all_user)
		return all_user

	delete = Twi_output.output_delete_user()
		
	print len(delete)



	user = __get_all_user(network)

	user = list(set(user) - set(delete))
	print len(user)
	
	for i in user:
		params = {"screen_name":i,"count":200}
		"ツイート取得"
		tweets = TM.get_api_data(url.get_tweet,params,twitter)
		if isinstance(tweets,int) == False:#非エラー
			docs = []
			for j in tweets:
				tweet = Normalize.extract_text(j)
				tweet = Normalize.normalize_text(tweet)
				docs.append(tweet)

			"""データベース書き込み"""
			Twi_input.input_tweet(i,docs)
			print "get tweets -> {0}...".format(i)




		










if __name__ == "__main__":
	get_user_tweet("network_Teara_exe.db")