import csv
import re
import pandas as pd

from textblob import TextBlob
import pycld2 as cld2
from MyDB_pro import remoteDB

from nltk.corpus import stopwords
import string



###________ GLOBAL VARIABLES
#####____________________________________

class emojiFunction():
    
    ####-----------------------------------
    ####---------   Functions  ------------
    ####-----------------------------------
    def text_2_list(self,fAddress):
        mylist=[]
        with open(fAddress, 'r') as f:
            for line in f:
                mylist.append(line)
        return mylist
    #######-----------------
    def arrays_2_csv(self,fileAdr,ar1, ar2):
        df1 = pd.DataFrame({"emojiCode" : ar1, "emojiCount" : ar2})
        df1.to_csv(fileAdr, index=False)
    ###--------------------------------------------
    def matrix_2_csv(self,fileAdr,matrix):
        df1 = pd.DataFrame(matrix)
        df1.to_csv(fileAdr)
    ####-----------------------------------------------
    def purify_tweet_text(text_of_tweet):
            ###------- purify the tweeter text from end line and other things
            tweet_text=text_of_tweet
            # tweet_text=text_of_tweet.encode("utf-8",'backslashreplace').decode("utf-8",'ignore')
            tweet_text= tweet_text.replace('\n', ' ').replace('\r', '')
            tweet_text= tweet_text.replace('"', ' ')           
            tweet_text= tweet_text.replace("'", ' ')
            tweet_text= tweet_text.replace('\\', ' ')
            return tweet_text            
    
    ###-----------------------------------------------
    def check_for_emoji_existance(mytext):
        ##--- This functions checks for the possibility of having emoji in a text and returns true if we have
        p=re.compile('\d+')
        return p.search(mytext)
    
    ###------------------------------------------------
    def find_emoji(tweet_text,my_emoji):
        emoji_str=my_emoji.replace('\n','')
        if emoji_str=='\U0000002A\U0000FE0F\U000020E3':
            pass
        else:
            match=re.search(emoji_str.decode('unicode-escape'),tweet_text)
            if match:
                #print count,': ', emoji_str, '  in -->>>>   ', tweet_text.encode("utf-8")
                print 'we have match: ', match.start()
    ###--------------------------------------------------
    def creat_emoji_List(self,emoji_file):
        ####  Read the list of emojis from a file and turn it to a list
        #emojiList00=text_2_list(emojiListFile)## '\U0000002A\U0000FE0F\U000020E3' is deleted from this text, BE CAREFUL!!!
        emojiList00 = [x for x in self.text_2_list(emoji_file) if x != u'\U0000002A\U0000FE0F\U000020E3']
        emojiList00.remove('\U0000002A\U0000FE0F\U000020E3')
        emojiList00=[emoji.replace('\n','') for emoji in emojiList00]
        return emojiList00
    ###---------------------------------------------------
    def create_Emoji_info_Dictionary(self,tweet, emojiMentionedList, emoji_Net_Index, emoji_Text_Index):
        ## In this function we create a dictionary from tweet information to save them in our hdf file
        tweet_words=self.filter_words(tweet['text']) ## to extract the words from tweet
        if len(tweet_words)==0:
            tweet_words=['0']
        
        hashtags=self.extract_hash_tags(tweet['text'])
        if len(hashtags)==0:
            hashtags=['0']
        tweet_Time='0'
        if 'timestamp_ms' in tweet:
            tweet_Time=str(tweet['timestamp_ms'])
        #---------
        if tweet.get('place')==None:
            countryCode='0'
        elif tweet.get('place').get('country_code')==None:
            countryCode='0'
        else:
            countryCode=str(tweet.get('place').get('country_code'))
        #--------
        if tweet.get('coordinates')==None:
            coordinates='0'
        elif tweet.get('coordinates').get('coordinates')==None:
            countryCode='0'
        else:
            coordinates=str(tweet.get('coordinates').get('coordinates'))
        #--------
        if tweet.get('user')==None:
            user_Location='0'.encode('utf-8').decode('utf-8')
        elif tweet.get('user').get('location')==None:
            user_Location='0'.encode('utf-8').decode('utf-8')
        else:
            user_Location=tweet.get('user').get('location').encode('utf-8').decode('utf-8')
        #--------
        if tweet.get('lang')==None:
            lang_Tag='0'
        else:
            lang_Tag=tweet.get('lang')
        #--------
        if tweet.get('in_reply_to_user_id_str')==None:
            in_reply_to_user='0'
        else:
            in_reply_to_user=tweet.get('in_reply_to_user_id_str')
        #--------
        user_mention=[]
        if tweet.get('entities')==None:
            user_mention=['0']
        elif tweet.get('entities').get('user_mentions')==None:
            user_mention=['0']
        else:
            for user in tweet.get('entities').get('user_mentions'):
                user_mention.append(user.get('id_str'))
        #--------------
        temp_dict={
                'tweet_ID':str(tweet['id']),
                'user_ID':tweet.get('user').get('id'),
                'tweet_Time':tweet_Time,
                'created_at':str(tweet['created_at']),
                'emoji_List':emojiMentionedList,
                'emoji_index_in_Net':emoji_Net_Index, 
                'emoji_position_in_text':emoji_Text_Index,
                'countryCode':countryCode,
                'coordinate':coordinates,
                'user_Location':user_Location,
                #'Lang_text':detect_language(mytweet['text'].encode('utf-8')),
                'tweet_lang':lang_Tag, 
                #'retweet_count':tweet['retweet_count'],
                'sentiment': self.get_tweet_sentiment(tweet['text']),
                'user_mention':user_mention,
                'in_reply_to_user':in_reply_to_user,
                'hashtags':hashtags,
                'words':tweet_words
                }
        return temp_dict
    ###----------------------------------------------------
    def create_Emoji_heap_Dictionary(self,tweet_count, tweet_with_emoji_count, emoji_seen_count, new_emoji_count, lang_tweet):
        temp_dict={
        'tweet_count':tweet_count,
        'tweet_with_emoji_count':tweet_with_emoji_count,
        'emoji_seen_count':emoji_seen_count,
        'new_emoji_count':new_emoji_count,
        'lang_tweet_emoji_tuple':(lang_tweet,tweet_with_emoji_count, new_emoji_count) 
                }
        return temp_dict
    ###----------------------------------------------------
    def find_frequent_words(myString, subString, frequency):
        return myString
    ###----------------------------------------------------
    def clean_tweet(self,tweet_text):
        """
        Utility function to clean tweet text by removing links, special characters
        using simple regex statements.
        :param tweet_text:
        :return: text of a tweet without special characters
        """

        return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet_text).split())
    
        
    ###---------------------------------------------------------------
    def get_tweet_sentiment(self,tweet_text):
        '''
        Utility function to classify sentiment of passed tweet
        using textblob's sentiment method
        '''
        # create TextBlob object of passed tweet text
        analysis = TextBlob(self.clean_tweet(tweet_text))
        # set sentiment
        return analysis.sentiment.polarity
        #if analysis.sentiment.polarity > 0:
        #    return 'positive',analysis.sentiment.polarity
        #elif analysis.sentiment.polarity == 0:
        #    return 'neutral',analysis.sentiment.polarity
        #else:
        #    return 'negative',analysis.sentiment.polarity
    ###----------------------------------------------
    def update_emoji_trace(emoji_trace, my_emoji):
        ### this function update emoji heap with this structure:
        ### emoji_heap={'seen_list':[], 'emoji_seen_count':0, 'new_emoji_count':0}
        if my_emoji in emoji_trace['seen_list']:
            emoji_trace['emoji_seen_count'] +=1
        else:
            emoji_trace['seen_list'].append(my_emoji)
            emoji_trace['emoji_seen_count']+=1
            emoji_trace['new_emoji_count']+=1
        return emoji_trace
    ###---------------------------------------------
    def update_emoji_heap(my_heap, emoji_trace, t_e_counter, t_counter, t_lang):
        ### this function update emoji heap with this structure:
        ### emoji_trace={'seen_list':[], 'emoji_seen_count':0, 'new_emoji_count':0}
        ### emoji_heap={'tweet_count':[],'emoji_seen_count':[], 'new_emoji_count':[]}
        my_heap['tweet_count'].append(t_counter)## number of tweet seen With/Without emoji
        my_heap['tweet_with_emoji_count'].append(t_e_counter)## number of tweet seen with emoji
        my_heap['emoji_seen_count'].append(emoji_trace['emoji_seen_count'])
        my_heap['new_emoji_count'].append(emoji_trace['new_emoji_count'])
        my_heap['lang_tweet_emoji_tuple'].append((t_lang, t_e_counter, emoji_trace['new_emoji_count']))
        return my_heap
    ###------------------------------------------------
    def create_tweet_words(self,tweet_text):
        return self.clean_tweet(tweet_text)
    ###-------------------------------------------------
    def update_adj_matrix(self,old_matrix, matIndex, textIndex):
        for k in range(len(matIndex)):
            for j in range(k+1,len(matIndex)):## It does not count the emoji itself, it counts the self edges
                if textIndex[k]<textIndex[j]:
                    #print 'link updated: ', matIndex[k], ' --> ', matIndex[j]
                    old_matrix[matIndex[k], matIndex[j]] += 1
                else:
                    #print 'link updated: ', matIndex[j], ' --> ', matIndex[k]
                    old_matrix[matIndex[j], matIndex[k]] += 1
        return old_matrix
    ###-------------------------------------------------
    def order_indexWise(self,index_net, index_text):
        ## This function recieve two arrays and sorts first one based on the second one
        ordered_list=index_net[:] ## copy by value, not by reference
        temp_text_L=index_text[:] ## copy by value, not by reference
        for i in range(len(index_net)):
            for j in range(i+1,len(index_net)):
                if temp_text_L[i]>temp_text_L[j]:
                    temp_text_L=self.swap_items(temp_text_L,i,j)
                    ordered_list=self.swap_items(ordered_list,i,j)
        return ordered_list                
    ###-------------------------------------------------
    def swap_items(my_list, index_1, index_2):
        temp=my_list[index_1]
        my_list[index_1]=my_list[index_2]
        my_list[index_2]=temp
        return my_list
    ###--------------------------------------------------
    def update_heap_mat(self,heap_matrix, ordered_list):
        """
        This function receives a heap matrix and an ordered list of indecies of the matrix and
        creates link between from the previous item in the list to the next one
        :param heap_matrix: The heap matrix of
        :param ordered_list:
        :return:
        """
        for i in range(len(ordered_list)-1):
            heap_matrix[ordered_list[i],ordered_list[i+1]]+=1
            #print 'in heap: ', ordered_list[i], '--->', ordered_list[i+1]  ## print to tset the heap 
        return heap_matrix
    ###------------------------------------------------
    def extract_hash_tags(self,my_text):
        return set(part[1:] for part in my_text.split() if part.startswith('#'))
    ###--------------------------------------------------
    #####################################################
    ###--------------------------------------------------
    #parameter: text:text of tweet as string
    #output: a list of words containing keywords in tweet along with their position in the original text
    #Note: every word in list is lower cased, no punctuation, no emojis
    def filter_words(self,tweet_text):
        #create templates for detecting emoticons and tokens
        emoticons_str = r"""
        (?:
            [:=;] # Eyes
            [oO\-]? # Nose (optional)
            [D\)\]\(\]/\\OpP] # Mouth
        )"""
        
        regex_str = [
            emoticons_str,
            r'<[^>]+>', # HTML tags
            r'(?:@[\w_]+)', # @-mentions
            r"(?:\#+[\w_]+[\w\'_\-]*[\w_]+)", # hash-tags
            r'http[s]?://(?:[a-z]|[0-9]|[$-_@.&amp;+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+', # URLs
        
            r'(?:(?:\d+,?)+(?:\.?\d+)?)', # numbers
            r"(?:[a-z][a-z'\-_]+[a-z])", # words with - and '
            r'(?:[\w_]+)', # other words
            r'(?:\S)' # anything else
        ]
        tokens_re = re.compile(r'('+'|'.join(regex_str)+')', re.VERBOSE | re.IGNORECASE)
        emoticons_re = re.compile(r'^'+emoticons_str+'$', re.VERBOSE | re.IGNORECASE)
        #convert string into list of words and keep track of indexes
        tokens=self.tokenize(tweet_text,tokens_re,emoticons_re)
        #filter words
        processed=self.process_tokens(tokens,tokens_re,emoticons_re)
        return(processed)
    
    
    #This function should not be called directly
    #parameter: s: a string
    #parameter: t_re: template for tokens
    #parameter: e_re: template for emoticons
    #output: a list of all words and their positions in s
    def tokenize(self,s,t_re,e_re):
        tokens=t_re.findall(s)
        inds=list()
        for token in tokens:
            inds.append([token,s.find(token)])
        return(inds)
    
        
    #This function is not directly called
    #parameter: tokens: a list of words with indexes
    #paremeter: t_re and e_re : templates for words       
    #output: a list of filtered strings with their index
    def process_tokens(self,tokens,t_re,e_re):
        processed=list()
        #add emoticons
        for token in tokens:
            if e_re.search(token[0]):
                processed.append(token)
            #remove links/urls
            if token[0].startswith('@') or token[0].startswith('http') :
                continue
            #remove single punctuation marks and emoji code points
            elif token[0] not in string.punctuation and len(token[0])>2:
                proc_token = "".join(l for l in token[0] if l not in string.punctuation)
                proc_token=proc_token.lower()
                #remove stop words
                if proc_token not in stopwords.words():
                    processed.append((proc_token,token[1]))
        return processed
    
