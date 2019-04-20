import threading
import socket
import time
import sys
import pickle
import json
max_nodes=8
fingertablesize=3

#FLAGS
#RQ=request
#RP=reply



class node:
    def __init__(self,successor,predecessor,data,fingertable,port):
        self.port=port       
        self.data=[] 
        self.key=port%max_nodes
        self.successor=self.key 
        self.successorport=self.port
        self.grandsuccessorkey=self.key
        self.grandsuccessorport=self.key
        self.predecessor=self.key 
        self.predecessorport=self.port
        self.fingertable=[]

    def print_values(self):
       print("Printing Node Values")
       print("Port Number is ", self.port) 
       print("Current Node Key is ", self.key) 
       print("Successor  is ", self.successor) 
       print("Successor Port",self.successorport)         
       print("grandsuccesor",self.grandsuccessorkey)
       print("grandsuccessorport",self.grandsuccessorport)
       print("Predecessor is ", self.predecessor) 
       print("Predecessor Port is ", self.predecessorport)
    def print_fingertable(self):

        print("Printing Finger Table of Node with key",self.key)
        for i in range(fingertablesize):
            print(self.fingertable[i])

    def FindSuccessor(self,message):
        print("Inside Find Successor") 
        if self.successor==self.key:
           message['found']="T"
           #message['successor']=str(self.successor)
           message['key']=str(self.key)
           message['port']=str(self.port) 
           message['grandsuccessorport']=self.key
        elif self.successor>int(message['otherkey']) and int(message['otherkey'])>=self.key:     #otherkey refers to key of the requested node        
           message['found']="T"
           
           message['key']=str(self.successor)
           message['port']=str(self.successorport)
           message['grandsuccessorport']=self.grandsuccessorport
           print("Found successor",message)
        elif self.successor==0 and self.key<=int(message['otherkey']):
           print("New node will be the last node in the ring")
           message['found']="T"
           #message['successor']=str(self.successor)
           message['key']=str(self.successor)
           message['port']=str(self.successorport)
           message['grandsuccessorport']=self.grandsuccessorport 
        else:                          
           message['found']="F"
           #message['successor']=str(self.successor)
           print("Found false")
           message['key']=str(self.successor)
           message['port']=str(self.successorport)
           message['grandsuccessorport']=self.grandsuccessorport
   
        print("messge from find successor", message)

        return message

    def join(self,node):
        print("First Find Successor and call node")

    def listening(self):
        print ("I am going to start listening for msg on PORT",self.port) 
        s=socket.socket()
        s.bind(('localhost',self.port))
        s.listen(10)  
        while True: 
            c, addr =s.accept()
            message=(c.recv(1024))
            message=json.loads(message.decode('ascii')) 
            print("I am node with port",self.port, "I am have received message from", message["otherport"])
            print("Complete message received", message)
            if message["type"]=="join":
                message=self.FindSuccessor((message))
                message['type']="reply" 
                c.send(json.dumps(message).encode('ascii'))
            elif message["type"]=="getpred": #This node predecessor will be  sent back
                if self.predecessor!=None: #If there exists a predecessor
                    print("Inside Successor") 
                    print(message)
                    oldpred=self.predecessor 
                    oldpredport=self.predecessorport
                    self.predecessor=int(message["otherkey"]) #setting successor's node pred as new node
                    self.predecessorport=int(message["otherport"]) #setting successor's node pred portnumber as new node 
                    message["type"]="replypred"
                    message["otherkey"]=oldpred
                    message["otherport"]=oldpredport 
                    self.print_values()
                    c.send(json.dumps(message).encode('ascii'))
                else:#if there is no predecessor 
                    pred=self.key 
                    predport=self.port  
                    self.predecessor=int(message["otherkey"]) #setting successor's node pred as new node
                    self.predecessorport=int(message["otherport"]) #setting successor's node pred portnumber as new node 
                    message["type"]="replypred"
                    message["otherkey"]=pred
                    message["otherport"]=predport 
                    
                    c.send(json.dumps(message).encode('ascii'))
            elif message["type"]=="updatepredsucc": #updating successor's predecoessor
                print("Inside UpdatePredSucc")
                self.successor=int(message["otherkey"])
                self.successorport=int(message["otherport"])
                message['type']="complete"
                
                self.print_values()
                print(message)
                c.send(json.dumps(message).encode('ascii'))
            elif message["type"]=="getsuccessport":
                 print("I am inside getsuccessport")
                 print("I am node, ", self.port," and my successor is ",self.successorport)
                 message["key"]=self.successor  #getting key of the successor
                 message["port"]=self.successorport   #getting port of the successor              
                 c.send(json.dumps(message).encode('ascii'))
            elif message["type"]=="nexthighestnodeid": 
                print("I am trying to find successor for id:", message["otherkey"])
                message=self.FindSuccessor((message))     
                c.send(json.dumps(message).encode('ascii'))
            elif message["type"]=="buildfingertable":
                print("Rebuilding fingertable for node with id", self.key)
                self.build_fingertable(message)
                c.send(json.dumps(message).encode('ascii'))
            elif message["type"]=="updategrand":
                self.update_grandsuccessor(message)
                c.send(json.dumps(message).encode('ascii'))
                


    def update_grandsuccessor(self,message):
        print("Getting GrandSuccessor")
        if self.successor!=self.key:
            message["type"]="getsuccessport"
            message["key"]=self.successor
            message["port"]=self.successorport
            message=self.sendmessage(message) 
            self.grandsuccessorkey=int(message["key"])
            self.grandsuccessorport=int(message["port"])
        self.print_values()
        return  
    
    def sendmessage(self,message):
        s=socket.socket() 
        print('I am node with port', self.port, 'sending message to port, ',message['port'], 'message type', message['type']) 
        messagesentto=message['port']
        s.connect(('localhost',(int(message['port']))))
        s.send(json.dumps(message).encode('ascii'))
        message=s.recv(1024)             #waiting for reply from requested node 
        message=json.loads(message.decode('ascii')) 
        print('received message from port :', messagesentto , 'message type', message['type']) 
        s.close()
        return message 


    #This checks for reply from the requested node join
    def reply(self,message):
        if message["found"]=="T":   #found successor
            print("Inside Reply") 
            print(message)
            self.successor=int(message["key"] )#key here refers to key of successor node
            self.successorport=int(message["port"])           
            message['type']="getpred"       #get new node predecessor from successor predecessor and set successor pred             
            message['otherkey']=self.key    #setting otherkey as current's node key
            message['otherport']=self.port #setting otherport as current's node port 
            
            message=self.sendmessage(message)  #sending msg to successor to update his predecessor and get old predecessor
            self.predecessor=int(message["otherkey"]) #Updating current node predecessor 
            self.predecessorport=int(message["otherport"]) 
            #Now we need to update old predecessor nodes
            message['type']="updatepredsucc"             
            message['otherkey']=self.key    #setting otherkey as current's node key
            message['otherport']=self.port #setting otherport as current's node port 
            message['key']=int(self.predecessor)#Sending message to predecessor to update his successor
            message['port']=int(self.predecessorport)
            
         
            self.sendmessage(message) #message send to successor's predecessor to update his successor
            return message
        elif message["found"]=="F":
             message['type']="join"
             return message 

    def build_fingertable(self,message):
        self.fingertable=[]
        for i in range(fingertablesize): 
            message['found']="F" 
            eachentry={}
            eachentry['range']=""
            eachentry['successorkey']=""
            eachentry['successorport']=""
            message['key']=str(self.key) #starting node from where to look for the successor
            message['port']=str(self.port) #starting port from where to look for the port
            ranges=self.key+2**i
            message['otherkey']=str(ranges)
            while message['found']=="F":
                print('sending message again')
                print(message)
                message['type']='nexthighestnodeid'
                if message['key']==str(self.key):
                    print("when i am equal to my key")
                    message=self.FindSuccessor(message)
                else:
                    message=self.sendmessage(message)
                    print("here")
                    print(message)
                if message['found']=="F":  #if successor is not found, search from successor 
                    message['key']=message['key'] #key contains next successor node
                    message['port']=message['port']
            print("Finger table entry for id", ranges)
            eachentry['range']=str(ranges)
            print('message is', message)
            eachentry['successorkey']=(message['key'] )
            eachentry['successorport']=str(message['port'])         
            self.fingertable.append(eachentry)
        print("complete fingertable")
        self.print_fingertable()
    
    def update_fingertables(self,message):
        currentkey=str(self.key)
        successor=str(self.successor)
        print("current key is ", currentkey)
        print("successor key is", successor)
        successorport=str(self.successorport)
        while currentkey!=successor: #if current key becomes equal to current key, all nodes are updated
            message["key"]=str(successor) 
            print(" Sending message to ",message["key"])
            message["port"]=str(successorport) 
            message["type"]="buildfingertable"
            message=self.sendmessage(message)
                       
            print("Rebuilding of finger table for node with id",str(successor)," completed")
            message["key"]=str(successor) 
            message["port"]=str(successorport) 
            message["type"]= "getsuccessport"
            message=self.sendmessage(message) #getting key of the successor's successor
            print("Printing Message")
            print(message)
            print("successor key is", message["key"])
             
            successor=str(message["key"]) #getting key of the successor's successor
            successorport=str(message["port"])
        print("updation completed")
    
    def updateallgrandsuccessors(self,message):
        currentkey=str(self.key)
        successor=str(self.successor)
        successorport=str(self.successorport)
        while currentkey!=successor: #if current key becomes equal to current key, all nodes are updated
             message["key"]=str(successor) 
             message["port"]=str(successorport)
             message["type"]="updategrand" 
             message=self.sendmessage(message)
             message["key"]=str(successor) 
             message["port"]=str(successorport) 
             message["type"]= "getsuccessport"
             message=self.sendmessage(message) #getting key of the successor's successor
             successor=str(message["key"]) #getting key of the successor's successor
             successorport=str(message["port"])

def main():

    message={
            'type':"",
            'port':"", 
            'key':"",
            'grand_successor_port':"",
            'found':"",
            'successor':"",
            'otherport':"",
            'otherkey':"",
            }
    MYPORT=int(sys.argv[1]) 
    print("I am a new node and right now i don't know about my successors or predessors ", MYPORT) 
    mynode=node(None,None,None,None,MYPORT)
    HOST='10.130.51.225'
    print ('I have started listening for messages from other node') 
    try: 
        listenerthread=threading.Thread(target=mynode.listening,args=())
        listenerthread.start() 
        choice=input("Press 1 if you are a new node, or Press 2 if join existing network?")
        if choice=="1":
            print("I am the first node")
            #mynode.build_fingertable(message)
            mynode.update_grandsuccessor(message)     
            mynode.print_values()
        else: 
            destport=input("Which node you want to send request for join")  
            key=int(destport) % max_nodes
            message['type']="join"
            message['key']=str(key)
            message['port']=str(destport)
            while message['type']=="join":
                message["otherkey"]=str(MYPORT%max_nodes)
                message["otherport"]=str(MYPORT)
                message=mynode.sendmessage(message) 
                if message['type']=="reply":
                   message=mynode.reply(message)
            print("I am added to the system")
            mynode.update_grandsuccessor(message)      
            mynode.updateallgrandsuccessors(message)     
            mynode.print_values()
            #mynode.build_fingertable(message) 

            #mynode.update_fingertables(message)
                
        choice=input("If you want to put a file press 1, or else press 2?")
        


        while True: 
            listenerthread.join(1) 
    except KeyboardInterrupt:
        sys.exit(1)

if __name__=='__main__':
    main()
