import socket
import sys
sys.path.append('/home/vchaska1/protobuf/protobuf-3.5.1/python')
import bank_pb2
import math
from random import randint
import time
port = 9090

if __name__ == '__main__':
    ip = socket.gethostbyname(socket.gethostname())
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address = (ip, port);
    s.bind(server_address)
    s.listen(1)
    
    total_money = int(sys.argv[1])
    branches = sys.argv[2]
    b_counter = 0
    s_counter = 0
    b_names = []
    b_ips = []
    b_ports = []
    r_counter = 0

    # Parse branches.txt
    with open(branches, 'r') as b:
        for line in b:
            b_counter = b_counter + 1
            words = line.split()
            b_names.append(words[0])
            b_ips.append(words[1])
            b_ports.append(int(words[2]))

    # Create the InitBranch message
    amount = math.floor(total_money / b_counter)
    d = total_money - int(amount) * b_counter
    init_msg = bank_pb2.InitBranch()
    init_msg.balance = int(amount)

    # Populate its fields
    for i in range(0, b_counter):
        branch = init_msg.all_branches.add()
        branch.name = b_names[i]
        branch.ip = b_ips[i]
        branch.port = b_ports[i]

    branch = init_msg.all_branches.add()
    branch.name = 'controller'
    branch.ip = ip
    branch.port = port

    # Wrap it inside BranchMessage object
    msg = bank_pb2.BranchMessage()
    msg.init_branch.CopyFrom(init_msg)

    # Send init message over the socket to each branch
    for i in range(0, b_counter):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_address = (b_ips[i], b_ports[i])
        sock.connect(server_address)

        try:
            #print 'Init message sent'
            if i == b_counter - 1:
                msg.init_branch.balance = msg.init_branch.balance + d
            sock.sendall(msg.SerializeToString())
        except:
            print 'Controller could not send the init message'
        finally:
            #print 'Socket closed by the controller'
            sock.close()
    
    # Send init Snaphot and retrieve Snaphot, wait for the response
    while True:
        time.sleep(10)
        # Send init snapshot
        dest = randint(0, b_counter - 1)
        s_msg = bank_pb2.InitSnapshot()
        s_msg.snapshot_id = s_counter

        msg = bank_pb2.BranchMessage()
        msg.init_snapshot.CopyFrom(s_msg)

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_address = (b_ips[dest], b_ports[dest])
        sock.connect(server_address)

        try:
            #print 'Init Snaphot message sent to ' + b_names[dest]
            sock.sendall(msg.SerializeToString())
        except:
            print 'Controller could not send the init snapshot message'
        finally:
            # print 'Socket closed by the controller'
            sock.close()

        time.sleep(2)
        # Send retrieve snapshot
        r_msg = bank_pb2.RetrieveSnapshot()
        r_msg.snapshot_id = s_counter

        msg = bank_pb2.BranchMessage()
        msg.retrieve_snapshot.CopyFrom(r_msg)

        for i in range(0, b_counter):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_address = (b_ips[i], b_ports[i])
            sock.connect(server_address)

            try:
                #print 'Retrieve message sent'
                sock.sendall(msg.SerializeToString())
            except:
                print 'Controller could not send retrieve message'
            finally:
                # print 'Socket closed by the controller'
                sock.close()

        # Wait for return snapshot
        print '\nSnapshot_id: ' + str(s_counter)
        total = 0
        while True:
            connection, client = s.accept()
            try:
                while True:
                    data = connection.recv(1024)
                
                    if data:
                        msg = bank_pb2.BranchMessage()
                        msg.ParseFromString(data)

                        if msg.HasField('return_snapshot'):
                            #print 'return msg received'
                            if msg.return_snapshot.local_snapshot.snapshot_id != s_counter:
                                raise Exception('Error in response receiver')
                            
                            names = []
                            for i in b_names:
                                if i != b_names[r_counter]:
                                    names.append(i)

                            print b_names[r_counter] + ': ' + str(msg.return_snapshot.local_snapshot.balance)
                            #print '--> ' + str(msg.return_snapshot.local_snapshot.balance)
                            total = total + msg.return_snapshot.local_snapshot.balance
                            
                            #for i in msg.return_snapshot.local_snapshot.channel_state:
                            #    print '    ' + str(i)

                            
                            counter = 0
                            for i in msg.return_snapshot.local_snapshot.channel_state:
                                print '    ' +  names[counter] + ' -> ' + b_names[r_counter] + ': ' + str(i)
                                counter = counter + 1

                            
                            dif = b_counter - counter - 1
                            for i in range(0, dif):
                                print '    ' + names[counter] + ' -> ' + b_names[r_counter] + ': ' + str(0)
                                counter = counter + 1
                            
    
                            r_counter = r_counter + 1
                        else:
                            print 'Incoming data has wrong protobuf format'
                    break
            finally:
                connection.close()
                if r_counter == b_counter:
                    #print 'Total Local: ' + str(total)
                    r_counter = 0
                    break

        s_counter = s_counter + 1
