# emoji
This program reads a dataset of tweets from a Mongodb server and extract emojis along with some 
useful information on hdf files.

We assume that the data (tweets in our case) is stored in a Mongodb database.
In the emoji_parallel.py file:

1- The program connects to either a remote or local Mongodb server

2- The program can be run in parallel or just on one process. 

3- The program uses an object from emoji_retrieveClass to extract emojis.

4- The program can be run on multi processors and generate .hdf files.


outputfiles:
Two types of files:
info_number1_to_number2.hdf
matrix_number1_to_number2.hdf

Based on the number of processes you will have hdf files. 
For example, if you have 4 processes, you will have 8 files (4 info + 4 matrix)
Then you can merge the .hdf files. 

The matrix_...hdf files contain the adjacency matrix of the emojis.
The info...hdf files contain key information including the following items:


	tweet_ID
    user_ID
    tweet_Time
    created_at
    emoji_List (the list of emojis detected from the tweet)
    emoji_index_in_Net (index of emojis in our adjacency matrix)
    emoji_position_in_text
    countryCode (countryCode of the user tweeted)
    coordinate  (coordinates of the user)
    user_Location (name of the location from which the user tweeted)
    tweet_lang  (language of the tweet detected by Twitter)
    sentiment (sentiment of the tweet)
    user_mention
    in_reply_to_user
    hashtags (important hashtags inside the tweet)
    words (important work inside the tweet)

In this version, we also calculate the heap of the emojis in order to investigate the heap's law for emojis.
The heap key is in the info...hdf file and it contains a dictionary as the following:

        temp_dict={
        'tweet_count':tweet_count,
        'tweet_with_emoji_count':tweet_with_emoji_count,
        'emoji_seen_count':emoji_seen_count,
        'new_emoji_count':new_emoji_count,
        'lang_tweet_emoji_tuple':(lang_tweet,tweet_with_emoji_count, new_emoji_count) 
                }
Please cite the following paper if you use this code in your research:

Seyednezhad, SM Mahdi, and Ronaldo Menezes. "Understanding Subject-Based Emoji Usage Using Network Science." In Workshop on Complex Networks CompleNet, pp. 151-159. Springer, Cham, 2017.

BibTex format:

	@inproceedings{seyednezhad2017understanding,
	  title={Understanding Subject-Based Emoji Usage Using Network Science},
	  author={Seyednezhad, SM Mahdi and Menezes, Ronaldo},
	  booktitle={Workshop on Complex Networks CompleNet},
	  pages={151--159},
	  year={2017},
	  organization={Springer}
	}


