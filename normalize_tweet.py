# -*- coding: utf-8 -*-
import re
import mojimoji

"""文書のクリーニング"""

def normalize_text(s):
	if isinstance(s,unicode) == False:
		s = unicode(s,"utf-8")
	s = mojimoji.zen_to_han(s,kana=False)
	s = re.sub(r"[\s]+","",s)
	s = re.sub(r"[˗֊‐‑‒–⁃⁻₋−]+","-",s)
	s = re.sub(r"[﹣－ｰ—―─━ー]+","-",s)
	s = re.sub(r"[']","",s)
	s = re.sub(r'["]',"",s)

	return s

	
"""無駄文字列削除"""
def extract_text(res):
	if res.has_key(u"entities"):
		entities = res[u"entities"]
		if entities.has_key(u"urls"):
			for url in entities[u"urls"]:
				res[u"text"] = res[u"text"].replace(url[u"url"], "")
		if entities.has_key(u"hashtags"):
			for hashtag in entities[u"hashtags"]:
				res[u"text"] = res[u"text"].replace("#" + hashtag[u"text"], "")
		if entities.has_key(u"symbols"):
			for symbol in entities[u"symbols"]:
				res[u"text"] = res[u"text"].replace("$" + symbol[u"text"], "")
		if entities.has_key(u"user_mentions"):
			for mention in entities[u"user_mentions"]:
				res[u"text"] = res[u"text"].replace("@" + mention[u"screen_name"], "")
		if entities.has_key("media"):
			for media in entities[u"media"]:
				res[u"text"] = res[u"text"].replace(media[u"url"], "")
		return res[u"text"]