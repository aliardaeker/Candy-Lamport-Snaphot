import socket
import sys
sys.path.append('/home/vchaska1/protobuf/protobuf-3.5.1/python')
import bank_pb2
import thread
import time
from random import randint
import math
import threading

balance_lock = threading.Lock()

class Branch:
    balance = 0
    name = sys.argv[1]
    port = int(sys.argv[2])
    ip = socket.gethostbyname(socket.gethostname())
    b_names = []
    b_ips = []
    b_ports = []
    b_counter = 0
    start_connecting = False
    current_snapshot_id = -1
    state_balance = 0
    incoming_names = []
    incoming_balances = []
    b_recording = []
    markers_going = False

def thread_sender(handler):
    addresses = []

    # Find addresses of all branches excpet itself
    while True:
        if handler.start_connecting:
            for i in range(0, handler.b_counter):
                if handler.b_names[i] != handler.name: 
                    address = (handler.b_ips[i], handler.b_ports[i])
                    addresses.append(address)
            break
    
    # Send message to periodically to every branch
    while True:
        if handler.markers_going:
            continue

        # print handler.name + ' has ' + str(handler.balance)
        amount = int(math.floor(randint(1, 5) * handler.balance / 100))
        # amount = 5
        if handler.balance - amount < 0:
            print handler.name + ' --> balance cannot be negative, will try again'
            continue

        dest = randint(0, handler.b_counter - 2)
        delay = randint(0,5)
        # delay = 0.05

        try:
            # print '     ' + handler.name + ' sends ' + str(amount) + ' to ' + str(addresses[dest])
            t = bank_pb2.Transfer()
            t.money = amount
            t.branch_name = handler.name

            msg = bank_pb2.BranchMessage()            
            msg.transfer.CopyFrom(t)

            balance_lock.acquire()
            if not handler.markers_going:
                handler.balance = handler.balance - amount
            balance_lock.release()
           
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect(addresses[dest])
            if not handler.markers_going:
                sock.sendall(msg.SerializeToString())
        except:
            print handler.name + ' could not send the message'
        time.sleep(delay)
        sock.close()

def thread_listener(sock, handler):
    init_received = False
    c_ip = ''
    c_port = 0

    # Listen for init message
    while True:
        connection, client = sock.accept()
        try:
            while True:
                data = connection.recv(1024)

                if data:
                    init_received = True
                    msg = bank_pb2.BranchMessage()
                    msg.ParseFromString(data)
                    handler.balance = msg.init_branch.balance 
                    
                    for i in msg.init_branch.all_branches:
                        handler.b_counter = handler.b_counter + 1
                        handler.b_names.append(i.name)
                        handler.b_ips.append(i.ip)
                        handler.b_ports.append(i.port)

                    handler.b_counter = handler.b_counter - 1
                    c_ip = handler.b_ips[handler.b_counter]
                    c_port = handler.b_ports[handler.b_counter]
                    handler.b_names.pop()
                    handler.b_ips.pop()
                    handler.b_ports.pop()
                else:
                    break
        finally:
            connection.close()
        if init_received:
            handler.start_connecting = True
            break

    # Listen for other messages
    while True:
        connection, client = sock.accept()
        try:
            while True:
                data = connection.recv(1024)
                
                if data:
                    msg = bank_pb2.BranchMessage()
                    msg.ParseFromString(data)

                    if msg.HasField('transfer'):
                        money = msg.transfer.money
                        # print handler.name + ' receives ' + str(money) + ' from ' + msg.transfer.branch_name
                        
                        # Update channel during snapshot algorithm 
                        # print '         ' + str(handler.b_recording)
                        # print '         ' + msg.transfer.branch_name

                        if msg.transfer.branch_name in handler.b_recording:
                            # print '     ' + handler.name + ' receives and appends ' + str(money) + ' from ' + msg.transfer.branch_name
                            handler.incoming_names.append(msg.transfer.branch_name)
                            handler.incoming_balances.append(money)
                        else:
                            # print '     ' + handler.name + ' receives ' + str(money) + ' from ' + msg.transfer.branch_name
                            balance_lock.acquire()
                            handler.balance = handler.balance + money
                            balance_lock.release()

                    elif msg.HasField('init_snapshot'):
                        # balance_lock.acquire()
                        handler.markers_going = True
                        # print handler.name + ' Init Snapshot received - local: ' +  str(handler.balance)
                        handler.state_balance = handler.balance
                        handler.current_snapshot_id = msg.init_snapshot.snapshot_id
                        
                        for i in range(0, handler.b_counter):
                            if handler.b_names[i] != handler.name:
                                handler.b_recording.append(handler.b_names[i])
                                addr = (handler.b_ips[i], handler.b_ports[i])
                                
                                try:
                                    m = bank_pb2.Marker()
                                    m.snapshot_id = handler.current_snapshot_id
                                    m.branch_name = handler.name
                                    
                                    msg = bank_pb2.BranchMessage()            
                                    msg.marker.CopyFrom(m)
           
                                    # print handler.name + ' sends marker to ' + handler.b_names[i]
                                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                                    s.connect(addr)
                                    s.sendall(msg.SerializeToString())
                                except Exception as e:
                                    print handler.name + ' could not send the marker message'
                                    print e
                                s.close()
                        handler.markers_going = False
                        # balance_lock.release()
                    elif msg.HasField('marker'):
                        # print 'marker msg received'
                        # Not first marker message
                        if handler.current_snapshot_id == msg.marker.snapshot_id:
                            # print handler.name + ' Second marker received from ' + msg.marker.branch_name
                            if msg.marker.branch_name in handler.b_recording:
                                handler.b_recording.remove(msg.marker.branch_name)
                        # First marker message
                        else:
                            # balance_lock.acquire()
                            handler.markers_going = True
                            mm = msg.marker.branch_name 
                            # print handler.name + ' First marker received from ' + mm + ' - local: ' + str(handler.balance)
                            handler.current_snapshot_id = msg.marker.snapshot_id
                            handler.state_balance = handler.balance
   
                            # Set marker`s channel to 0 and start recording other channels
                            handler.incoming_names.append(msg.marker.branch_name)
                            handler.incoming_balances.append(0)

                            for i in range(0, handler.b_counter):
                                n = handler.b_names[i]
                                if handler.name != n and mm != n:
                                    #print "         mm: " + mm
                                    #print "         record: " + n
                                    handler.b_recording.append(n)
                                # Send markers
                                if handler.name != n:
                                    addr = (handler.b_ips[i], handler.b_ports[i])        
                                    try:
                                        m = bank_pb2.Marker()
                                        m.snapshot_id = handler.current_snapshot_id
                                        m.branch_name = handler.name

                                        msg = bank_pb2.BranchMessage()            
                                        msg.marker.CopyFrom(m)
                                        
                                        # print handler.name + ' sends marker to ' + handler.b_names[i]
                                        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                                        s.connect(addr)
                                        s.sendall(msg.SerializeToString())
                                    except:
                                        print handler.name + ' could not send the marker message from first'
                                    finally:
                                        s.close()
                            handler.markers_going = False
                            # balance_lock.release()
                    elif msg.HasField('retrieve_snapshot'):
                        # print handler.name + ' Retrieve Snapshot received'

                        if msg.retrieve_snapshot.snapshot_id == handler.current_snapshot_id:
                            # Send return snapshot message
                            r = bank_pb2.ReturnSnapshot()
                            r.local_snapshot.snapshot_id = handler.current_snapshot_id
                            r.local_snapshot.balance = handler.state_balance

                            names = []
                            balances = []

                            for index, i in enumerate(handler.incoming_names):
                                if i not in names:
                                    names.append(i)
                                    balances.append(handler.incoming_balances[index])
                                else:
                                    ind = names.index(i)
                                    balances[ind] = balances[ind] + handler.incoming_balances[index]

                            # Populate channel states
                            for i in balances:
                            # for i in handler.incoming_balances:
                                r.local_snapshot.channel_state.append(i)
                            
                            msg = bank_pb2.BranchMessage()
                            msg.return_snapshot.CopyFrom(r)

                            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            server_address = (c_ip, c_port)
                            s.connect(server_address)

                            try:
                                # print handler.name + ' Response sent'
                                s.sendall(msg.SerializeToString())
                            except:
                                print 'Could not send response message'
                            finally:
                                s.close()
                        
                            balance_lock.acquire()
                            for i in balances:
                            # for i in handler.incoming_balances:
                                handler.balance = handler.balance + i
                            balance_lock.release()

                            handler.incoming_names = []
                            handler.incoming_balances = []
                            handler.b_recording = []
                        else:
                            print 'Error in retrieve snapshot - ids do not match'
                    else:
                        print 'Incoming data has wrong protobuf format'
                break
        finally:
            connection.close()
         
if __name__ == '__main__':
    handler = Branch()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address = (handler.ip, handler.port);
    sock.bind(server_address)
    sock.listen(1)

    try:
        t = thread.start_new_thread(thread_listener, (sock, handler, ))
        t = thread.start_new_thread(thread_sender, (handler, ))
    except:
        print 'Threads cannot be initiated'

    while True:
        pass
