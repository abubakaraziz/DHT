import threading
import socket
import time
import sys
import pickle
import json
max_nodes=8

#FLAGS
#RQ=request
#RP=reply



class node:
    def __init__(self,successor,predecessor,data,fingertable,port):
        self.port=port       
        self.data=data 
        self.successor=successor 
        self.successorport=None 
        self.grandsuccessorkey=None 
        self.grandsuccessorport=None
        self.predecessor=predecessor 
        self.predecessorport=None
        self.key=port%max_nodes

    def print_values(self):
       print("Printing Node Values")
       print("Port Number is ", self.port)
          
       print("Current Node Key is ", self.key)
       
       print("Successor  is ", self.successor)
        
       print("Successor Port",self.successorport)         
       print("Predecessor is ", self.predecessor)
               
       print("Predecessor Port is ", self.predecessorport)
       print("grandsuccesor",self.grandsuccessorkey)
       print("grandsuccessorport",self.grandsuccessorport)
   
    def FindSuccessor(self,message):
        print("Inside Find Successor") 
        if self.successor==None:
           message['found']="T"
           #message['successor']=str(self.successor)
           message['key']=str(self.key)
           message['port']=str(self.port) 
           message['grandsuccessorport']=None
        elif self.successor>int(message['otherkey']) and int(message['otherkey'])>self.key:     #otherkey refers to key of the requested node        
           message['found']="T"
           #message['successor']=str(self.successor)
           message['key']=str(self.successor)
           message['port']=str(self.successorport)
           message['grandsuccessorport']=self.grandsuccessorport
           print("Found successor",message)
        elif self.successor==0 and self.key<int(message['otherkey']):
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
            print ("Received Request From",) 
            print (c,addr)
            message=(c.recv(1024))
            print("mesage",message)
            message=json.loads(message.decode('ascii'))
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
                  
               
    
    def sendmessage(self,message):
        s=socket.socket() 
         
        s.connect(('localhost',(int(message['port']))))
        s.send(json.dumps(message).encode('ascii'))
        message=s.recv(1024)             #waiting for reply from requested node       
        message=json.loads(message.decode('ascii'))
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
            mynode.print_values()

        while True: 
            listenerthread.join(1) 
    except KeyboardInterrupt:
        sys.exit(1)

if __name__=='__main__':
    main()
