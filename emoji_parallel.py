
from multiprocessing import Pool
from MyDB_pro import *
from emoji_retrieveClass import emjRetrieveClass

# import pandas as pd

###________ GLOBAL VARIABLES_____________

#emojiFunction=ec.emojiFunction()

### If the dataset is huge, you may want to save whatever you after each period.
saving_period=40000 ##<<<<---------- The number of tweets that if the program reaches it will save the files.

### If it is a test, the program exits at the saving period
my_Stop=True

### FYI: Sometimes, you cannot install mongo as admin so you can just run and use the mongodb executable file locally.
### If the mongodb is local. we set mongo_local=True .
mongo_local=False   #####<<<<<<<----------------------IMPORTANT  LOCAL or REMOTE

### Number of processes we want to have.
divide=8   ####<<<<<<<<<<<<---------  NUMBER OF PROCESS or DATA division


chunk_Nomber=1 ## the part of the dataset that is supposed to pass to the emoji_find function; starts from 1
#------- The code will start the program from this entry of the mongo dataset. 
offset=0 ## ------------  This is used when program crashed at a certain point. It will be applied to all pchunks of data.

###---------->  DB variables  <------------
mongo_IP='localhost'
mongo_Port='27017'
db_Name=''
col_Name=''
user_Name=''
user_pass=''


#####  FILES ###-------------------
dir_Name=db_Name
output_directorty='output_files/'+dir_Name+'/'

## List of emojis in utf-8 unicode
emojiListFile='emojiList_2300.txt'
#db_Name=''##<<<-------------------------------------------------------------------<<<<<<<<<---------
###### data frame keys


concat_tweet= True
emoji_seen_list=[]

#######______________End of variables_________________
######################################################
##############  FUNCTIONS
def getCollection(userName, userpass, mongoIP, mongoPort, dbName, colName):
    database = remoteDB(userName, userpass, mongoIP, mongoPort, dbName, colName)
    collection = database.get_collection()
    return collection
###----------------------------------
def calculate_start_limit(my_db, my_div, offSet):
    """
    :param my_db: The database
    :param my_div: number of divisions
    :param offSet: starts from beginig or some offset
    :return: list of tuples containing the start (number of the first data) and end of each chunk of data.
    """
    my_limit=int(my_db.get_collection().find().count()/my_div)-offSet
    sl_list=[]
    count=0
    my_start=offSet
    while count<my_div:
        sl_list.append((my_start,my_limit))
        count+=1
        my_start+=my_limit+offSet
    return sl_list
  

#### END OF FUNCTIONS
#####################################################

### define and assign values to the dictionary containing the db information that will be passed to emojiRetreive class. 
my_db_dict={'mongo_IP':mongo_IP, 'mongo_Port':mongo_Port, 'user_Name':user_Name,
            'user_pass':user_pass, 'db_Name':db_Name, 'col_Name':col_Name }

if mongo_local:### if it is local it does not need any user or pass
    myDB = localDB(db_Name, col_Name)
    test_col= myDB.get_collection()
else:
    ### read from MongoDB
    myDB = remoteDB(user_Name, user_pass, mongo_IP, mongo_Port, db_Name,  col_Name)
    test_col=getCollection(user_Name, user_pass, mongo_IP, mongo_Port, db_Name,  col_Name)
count=0

print 'Db has been read: ', test_col.count()

#############  doing parallel
#chunk_Nomber-=1
# Let's set the limits of each chunck of data that we want to pass to exh processor
st_lim_list = calculate_start_limit(myDB, divide, offset)

## you may check the limits of each chunck by uncommenting the following
#print 'st_lim_list', st_lim_list
#for st in st_lim_list:
#    print '[{},{}]'.format(st[0],st[0]+st[1])
#print 'st_lim_list[{}]'.format(chunk_Nomber),st_lim_list[chunk_Nomber]


## Now let's define an object from emoji_retrieveClass too retrieve the emojis
emj_ret=emjRetrieveClass(concat_tweet, output_directorty, mongo_local, my_db_dict,
                         count_to_save=saving_period, my_stop=my_Stop)




## We define a function just to make things easier for parellel processing
def f(arg):
   return emj_ret.find_emoji_partial_multiFiles(arg)


pool=Pool(processes=divide)

pool.map(f, st_lim_list)
