# -*- coding: utf-8 -*-
import re
import pandas as pd
import numpy as np
import time
import emojiClass as ec
from MyDB_pro import *

###________ GLOBAL VARIABLES
#####____________________________________

## We use an object from emojiClass to manage things related to emojis, such as saving the information about emojis.

emojiFunction=ec.emojiFunction()

####------------------------------

class emjRetrieveClass:
### code variables
    def __init__(self,concat_tweet, result_folder, is_local, db_dict, count_to_save, my_stop):
        #emoji_csv=open(emojiInfoFile_address, 'wb')
        #csv_writer=csv.writer(emoji_csv, delimiter=',')
        self.emoji_list=emojiFunction.creat_emoji_List(emojiListFile)
        self.concat_tweet=concat_tweet
        #self.tweetDB=tweetDB
        #self.col=tweetDB.get_collection()
        self.result_dir=result_folder
        self.is_local=is_local
        self.db_dict=db_dict
        self.count_to_save=count_to_save
        self.stop=my_stop # stop after the test
        
    def get_collection(self):
        if self.is_local:
            self.tweetDB=localDB(self.db_dict['db_Name'], self.db_dict['col_Name'])
        else:
            self.tweetDB = remoteDB(self.db_dict['user_Name'], self.db_dict['user_pass'], self.db_dict['mongo_IP'], self.db_dict['mongo_Port'], self.db_dict['db_Name'], self.db_dict['col_Name'])
        collection = self.tweetDB.get_collection()
        return collection  
    
    def write_on_hdf(self, hdf_file, hdf_struct, hdf_key, my_mode):
        while True:
            try:
                hdf_struct.to_hdf(hdf_file, key=hdf_key, mode=my_mode)
                break
            except TypeError:
                time.sleep(60)
                pass

    #################################################

    def find_emoji_partial_multiFiles(self,bound_tuple):
        """
          This function receives start and limit from of the data base from mongo and extract the emoji from that part
          Finally it writes the data frame on an hdf file
        """
        count_2_save=self.count_to_save
        save_period=count_2_save
        start=bound_tuple[0]
        limit=bound_tuple[1]
        emoji_hdf5_Info_File_address = '{}/info_{}_to_{}.hdf'.format(self.result_dir, start+1, start+limit)
        emoji_hdf5_Mat_File_address = '{}/matrix_{}_to_{}.hdf'.format(self.result_dir, start+1, start+limit)
        trace_working_file = '{}/taceWorking_{}_to_{}.txt'.format(self.result_dir, start+1, start+limit)
        
        my_col=self.get_collection()
        part_DB=my_col.find().skip(start).limit(limit)
                
        emojiList=self.emoji_list
        adjMat = np.zeros((len(emojiList), len(emojiList)), dtype = int) # The matrix containing the edges
        emojiCount=np.zeros((len(emojiList)), dtype = int) # The number of emoji in the tweet dataset
        heap_mat = np.zeros((len(emojiList), len(emojiList)), dtype = int) # The matrix containing the edges
        last_emoji_netIndex=0
        df_emoji_info = pd.DataFrame()
        df_emoji_heap = pd.DataFrame()
        count_tweet=0
        count_tweet_emoji=0
        count_total_seen_emoji=0
        count_new_emoji=0
        ####------------------------------------------------------######
        ####------------------------------------------------------######
        #### This is the part that the emoji extractor works.
        #### It reads each tweet and matches teh emoji unicodes.
        #### If the emoji unicode is in the text, it will be appended to the "mentionedTogether" list.
        print 'Start to extract emojis.....'
        for mytweet in part_DB:
            mentionedTogether=[]  ## It stores the emojis detected from the current tweet (i.e. mytweet).
            mentionedTogether_index_in_Net=[] ## It stores the index of emojis. The indeices are defined based on the emojiList.
            mentionedTogether_position_in_Text=[] ## It stores the posision of emoji in the text for future work.
            count_tweet+=1
            if 'text' in mytweet:
                #count_tweet+=1
                for emoji in emojiList:
                    emoji_str=emoji.replace('\n','')
                    match_all=re.finditer(emoji_str.decode('unicode-escape'),mytweet['text'])
                    for match in match_all:
                        count_total_seen_emoji+=1
                        mentionedTogether.append(emoji)
                        mentionedTogether_index_in_Net.append(emojiList.index(emoji))
                        mentionedTogether_position_in_Text.append(int(match.start()))
                        emojiCount[emojiList.index(emoji)]+=1

        
                if len(mentionedTogether)>0:
                    ## Yoiu can uncomment the followings to see the tweets detected:
                    #print 'tweet #', count_tweet, ': ', mytweet['text']
                    #print mentionedTogether
                    #print '-----------------------------------------------------'
                    ##
                    count_tweet_emoji+=1
                    emoji_dict=emojiFunction.create_Emoji_info_Dictionary(mytweet,mentionedTogether, mentionedTogether_index_in_Net, 
                                                mentionedTogether_position_in_Text)## creating the dictionary of info
                    df_emoji_info = df_emoji_info.append(emoji_dict, ignore_index=True)## updating dataframe for info by emoji_info dictionary
                    emoji_heap_dict=emojiFunction.create_Emoji_heap_Dictionary(count_tweet, count_tweet_emoji, count_total_seen_emoji,
                                                count_new_emoji, mytweet['lang'])## creating the dictionary for heap
                    df_emoji_heap=df_emoji_heap.append(emoji_heap_dict, ignore_index=True)## updating dataframe for heap by heap dictionary
                
                if (len(mentionedTogether)>1):#######  2 Mentioned - If they are mentioned together they should be in this list
                    #print count_tweet,': ',mentionedTogether_index_in_Net, '(NET) is/are mentioned in: ', mytweet['text']
                    #print (mentionedTogether_position_in_Text, ' TEXT is/are mentioned in: ', mytweet['text'])
                    adjMat=emojiFunction.update_adj_matrix(adjMat, mentionedTogether_index_in_Net, mentionedTogether_position_in_Text)
                    if self.concat_tweet and count_tweet_emoji>1:
                        mentionedTogether_index_in_Net.insert(0,last_emoji_netIndex)
                    heap_mat=emojiFunction.update_heap_mat(heap_mat, mentionedTogether_index_in_Net)
                if len(mentionedTogether)>0:
                    last_emoji_netIndex=mentionedTogether_index_in_Net.pop()
                    
                if count_tweet>count_2_save:
                    count_2_save+=save_period
                    print 'total number of tweets: ',count_tweet, ' saving files .............'
                    #print (mentionedTogether_index_in_Net, '(NET) is/are mentioned in: ', mytweet['text'])
                    df_emoji_count= pd.DataFrame(data=emojiCount, index=emojiList)
                    
                    df_emoji_adjMatrix=pd.DataFrame(data=adjMat, index=np.arange(len(emojiList)), columns=np.arange(len(emojiList)))
                    df_emoji_heapMatrix=pd.DataFrame(data=heap_mat, index=np.arange(len(emojiList)), columns=np.arange(len(emojiList)))
                    
                    #df_emoji_adjMatrix=pd.DataFrame(data=adjMat, index=np.arange(len(emojiList)), columns=np.arange(len(emojiList))) ## create data frame for adjacency matrix
                    #df_emoji_heapMatrix=pd.DataFrame(data=heap_mat, index=np.arange(len(emojiList)), columns=np.arange(len(emojiList))) ## create dataframe for the heap matrix
                    print 'Saving df_info .........'
                    self.write_on_hdf(emoji_hdf5_Info_File_address, hdf_struct=df_emoji_info, hdf_key='df_info', my_mode='a')
                    print 'Saving df_heap ..........'
                    self.write_on_hdf(emoji_hdf5_Info_File_address, hdf_struct=df_emoji_heap, hdf_key='df_heap', my_mode='a')
                    del df_emoji_info
                    df_emoji_info = pd.DataFrame()
                    del df_emoji_heap
                    df_emoji_heap = pd.DataFrame()
                    
                    print 'Saving df_count .........'
                    self.write_on_hdf(emoji_hdf5_Mat_File_address, hdf_struct=df_emoji_count, hdf_key='df_count', my_mode='w')
                    print 'Saving df_adjMat ..........'
                    self.write_on_hdf(emoji_hdf5_Mat_File_address, hdf_struct=df_emoji_adjMatrix, hdf_key='df_adjMat', my_mode='a')
                    print 'Saving df_heapMat ..........'
                    self.write_on_hdf(emoji_hdf5_Mat_File_address, hdf_struct=df_emoji_heapMatrix, hdf_key='df_heapMat', my_mode='a')                    
                    
                    with open(trace_working_file, 'a') as the_file:
                        temp='\t'+str(count_tweet)+',\t'+str(mytweet['created_at'])+',\t'+str(mytweet['id'])
                        the_file.write(temp)
                        the_file.write('\n')
                    print 'After tweet #{}, the {}_to_{} part was saved'.format(count_tweet, start+1, start+limit)
                    print 'Working on the rest........'
                    if self.stop:
                        break

        print 'Saving files of the part {}_to{} for the last time...............'.format(start+1, start+limit)
        df_emoji_count= pd.DataFrame(data=emojiCount, index=emojiList)
        df_emoji_adjMatrix=pd.DataFrame(data=adjMat, index=np.arange(len(emojiList)), columns=np.arange(len(emojiList)))
        df_emoji_heapMatrix=pd.DataFrame(data=heap_mat, index=np.arange(len(emojiList)), columns=np.arange(len(emojiList)))
        
        #df_emoji_info.to_hdf(emoji_hdf5_Mat_File_address, where='df_info, df_heap, df_count, df_adjMat, df_heapMat', mode='w')
        
        self.write_on_hdf(emoji_hdf5_Info_File_address, hdf_struct=df_emoji_info, hdf_key='df_info', my_mode='a')
        self.write_on_hdf(emoji_hdf5_Info_File_address, hdf_struct=df_emoji_heap, hdf_key='df_heap', my_mode='a')
        self.write_on_hdf(emoji_hdf5_Mat_File_address, hdf_struct=df_emoji_count, hdf_key='df_count', my_mode='w')
        self.write_on_hdf(emoji_hdf5_Mat_File_address, hdf_struct=df_emoji_adjMatrix, hdf_key='df_adjMat', my_mode='a')
        self.write_on_hdf(emoji_hdf5_Mat_File_address, hdf_struct=df_emoji_heapMatrix, hdf_key='df_heapMat', my_mode='a')                    

        with open(trace_working_file, 'a') as the_file:
            temp='\t'+str(count_tweet)+',\t'+str(mytweet['created_at'])+',\t'+str(mytweet['id'])
            the_file.write(temp)
            the_file.write('\n')
        print "total emoji: ", count_total_seen_emoji
        # return {'df_emoji_info':df_emoji_info, 'df_emoji_heap':df_emoji_heap, 'df_emoji_count':df_emoji_count, 'df_emoji_adjMatrix':df_emoji_adjMatrix, 'df_emoji_heapMatrix':df_emoji_heapMatrix}
#################################################