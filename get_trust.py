# -*- coding: utf-8 -*-
import twitter_data_output as Twi_output
import twitter_data_input as Twi_input
import random
import json

"""RWRでの信頼度推定"""

#-------------------------------#
#手動変数あり                     #
#リスタート率　alphaに注意         #
#終了時 counter = 1000000設定    #
#DBname = "exist_{0}.db".format(start_user)
#-------------------------------#

def get_trust_list(database):
	network = Twi_output.output_network(database)
	start_user = unicode(database[8:-3],"utf-8")

	"""変数設定"""
	per = {}
	counter = 0
	alpha = 0.5
	"""-------"""

	def __judge_restart(alpha):
		rand = random.random()
		if rand < alpha:
			return True
		else:
			return False
	def __judge_exist_network(network,user):
		if network.has_key(user):
			return True
		else:
			return False
	def __randomget_followee(network,user):
		length = len(network[user])
		rand = random.randint(0,length-1)
		followee = network[user][rand]
		return followee
	def __get_probably(per,user):
		if per.has_key(user):
			per[user] = per[user] + 1
		else:
			per[user] = 1
		return per

	"""初期ユーザの設定"""
	user = start_user
	while 1:
		"""何回目?"""
		counter = counter + 1
		print counter
		"""リスタートの判断"""
		if __judge_restart(alpha):
			user = start_user

		"""ユーザのネットワークの存在確認"""
		if __judge_exist_network(network,user) == False:
			user = start_user

		"""Followeeの取得"""
		user = __randomget_followee(network,user)

		"""出現回数の記録"""
		per = __get_probably(per,user)


		"""途中停止用のデータサルベージ
		if counter % 100000 == 0:
			json_name = "{0}_probably.json".format(start_user)
			with open(json_name,"w") as f:
				json.dump(per,f,sort_keys = True, indent = 4)
		"""


		"""終了判定"""
		if counter >= 1000000:
			break

	"""DBへの書き込み"""
	DBname = "exist_{0}.db".format(start_user)
	Twi_input.F_input_exist_count(per,DBname,start_user)




if __name__ == "__main__":
	get_trust_list("network_Teara_exe.db")