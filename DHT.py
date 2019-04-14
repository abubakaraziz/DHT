import threading
import socket
import time
import sys
import pickle
max_nodes=5

#FLAGS
#RQ=request
#RP=reply
class node:
    def __init__(self,successor,predecessor,data,fingertable,port):
        self.port=port       
        self.data=data 
        self.successor=successor
        self.predecessor=predecessor
        self.fingertable=fingertable 
        self.key=port%max_nodes
    def FindSuccessor(self,requestid):
        requestid=int(requestid)
        if self.successor==None:
            self.succssor=requestid     #Making current node's successor =request id since if intially it has no successor and predecessor
            return True 
        elif self.successor>requestid and self.port<request.id:  # myport=5 successor=10  requestid=8 
            return False
        else:
            return False
        print("Find Succesor")


    def join(self,node):
        print("First Find Successor and call node")

    def listening(self,HOST):
        print ("I am going to start listening for msg on PORT",self.port) 
        s=socket.socket()
        s.bind((HOST,self.port))
        s.listen(10)  
        while True: 
            c, addr =s.accept()
            print ("Received Request From",) 
            print (c,addr)
            data=(c.recv(1024))
            data=data.decode('ascii')
            print(data[0:2])
            if data[0:2]=="RQ":
               value="N" 
               if value=="N": #if successor is N
                    message="RPN"+str(self.key)    
                    c.send(message.encode('ascii'))
    def sendrequest(self,HOST,sendrequesto):
        found=False
        while found==False:
            s=socket.socket()
            s.connect((HOST,int(sendrequesto)))
            key=int(sendrequesto) % max_nodes
            print("here")
            message="RQ"+str(key)+"" + str (self.port)   
            s.send(message.encode('ascii'))
            data=s.recv(1024)                  
            data=data.decode('ascii')
            print(data)
            found=True
            if data[0:2]=="RP":
                if data[2:3]=="T":
                    self.predecessor=int(data[3:4]) 
            s.close()




def main():
    MYPORT=int(sys.argv[1]) 
    print("I am a new node and right now i don't know about my successors or predessors ", MYPORT) 
    mynode=node(None,None,None,None,MYPORT)
    HOST='10.130.51.225'
    print ('I have started listening for messages from other node') 
    try: 
        listenerthread=threading.Thread(target=mynode.listening,args=(HOST,))
        listenerthread.start() 
        choice=input("Press 1 if you are a new node, or Press 2 if join existing network?")
        if choice=="1":
            print("I am the first node")

        else: 
           destport=input("Which node you want to send request for join")
           mynode.sendrequest(HOST,destport)           
        while True: 
            listenerthread.join(1) 
    except KeyboardInterrupt:
        sys.exit(1)

if __name__=='__main__':
    main()
