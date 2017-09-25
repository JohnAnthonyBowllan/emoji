'''
Created on Sep 20, 2015

@author: mehdi
'''
from pymongo import MongoClient


####################################
def makeMongoURL(usrName, usrpass, mongoIP, mongoPort, dbName):
	# To create the url to connect to a remote mongo
    mongoURL = 'mongodb://'+usrName + ':' + usrpass + '@' + mongoIP + ':' + mongoPort + '/' + dbName
    return mongoURL
####--------------------------------------------


class localDB:
    
    def __init__(self, db_Name, col_Name):
        print 'Connecting to: ', 'local, db_Name: ', db_Name, '--------- collection name: ', col_Name 
        self.client = MongoClient('localhost', 27017)
        self.db = self.client[db_Name]
        self.collection = self.db[col_Name]
        print 'connected'
        
    def get_collection(self):
        return self.collection
            
    def get_record(self):
        self.collection.find()
    
    def dbClose(self):
        self.collection.close()
    
    def client_close(self):
        self.client.close()


class remoteDB:
    
    def __init__(self, user_Name, user_pass, mongo_IP, mongo_Port, db_Name, col_Name ):
        print 'user_Name: ',user_Name, '--------   db_Name: ', db_Name, '--------- collection name: ', col_Name 
        self.uri = makeMongoURL(user_Name, user_pass, mongo_IP, mongo_Port, db_Name)
        self.client=MongoClient(self.uri)
        mycode='self.db = self.client.'+db_Name
        exec(mycode)
        self.collection = self.db[col_Name]

		
    def get_collection(self):
        return self.collection
	
    def dbClose(self):
        self.collection.close()
        
    def client_close(self):
        self.client.close()

