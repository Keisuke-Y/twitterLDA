# -*- coding: utf-8 -*-
import twitter_module as TM
import twitter_data_input as Twi_input
import twitter_data_output as Twi_output

#-----------------------#
#DBname = "network_{0}.db".format(screen_name)
#注意
#-----------------------#

"""followユーザの取得"""


def get_users(screen_name,database = 0):
	users = {}
	if database != 0:
		"""データベースがある時"""
		users = Twi_output.output_network("./DB/{0}".format(database))
		#print users["Teara_exe"]
		
	
	twitter = TM.Twitter()

	"""max_user決定　一人あたり500人までで"""
	max_user = 200
	"""-------------------------------"""
	def __get_follow(screen_name,users,twitter):
		if users.has_key(screen_name) == False:
			"""変数まとめ"""
			user_list = []
			counter = 0
			apiUrl = TM.ApiUrl()
			"""---------"""

			"""ユーザのフォローを抽出"""
			"""first_step"""
			param = {"screen_name":screen_name,"count":200}
			friends_list = TM.get_api_data(apiUrl.follow_list,param,twitter)
			if isinstance(friends_list,int):
				pass
			else:
				for i in friends_list[u"users"]:
					user_list.append(i[u"screen_name"])
					counter = counter + 1

				while 1:
					if friends_list[u"next_cursor"] != 0 and counter < 200:
						"""second_step    over 200"""
						param = {"screen_name":screen_name,"count":200,"cursor":friends_list[u"next_cursor"]}
						friends_list = TM.get_api_data(apiUrl.follow_list,param,twitter)
						if isinstance(friends_list,int):
							pass
						else: 
							for i in friends_list["users"]:
								if counter == 200:
									break
								else:
									user_list.append(i[u"screen_name"])
									counter = counter + 1
					else:
						break

			users[screen_name] = user_list

		return users

	"""first_user"""
	print "analize {0}...".format(screen_name)
	users = __get_follow(screen_name,users,twitter)



	"""second_user"""
	for user in users[screen_name]:
		print "analize {0}...".format(user)
		users = __get_follow(user,users,twitter)

	Twi_input.input_network_buff_json(users,"network_ffuser.json")
	"""followee's Followee's Followee"""
	for user in users[screen_name]:#起点ユーザのFollowee
		for i in users[user]:#FolloweeのFollowee
			print "analize fff user {0}->{1}->{2}".format(screen_name,user,i)
			users = __get_follow(i,users,twitter)
		Twi_input.input_network_buff_json(users,"network_ffuser.json")
	

	"""DBへの登録"""
	if database == 0:
		DBname = "network_{0}.db".format(screen_name)
		Twi_input.input_network(users,DBname)
	else:
		DBname = "network_{0}.db".format(screen_name)
		Twi_input.input_network_with_drop(users,DBname)



	
if __name__ == "__main__":
	get_users(u"Teara_exe")
