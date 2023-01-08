import random
import time

from parameters import *


class Node:

    def __init__(self):

        self.id = None  # Stores the id of the node
        self.predecessor = None  # Stores the predecessor of the node
        self.successor = None  # Stores the successor of the node
        self.finger_table = [None] * m  # Stores the finger table of the node
        self.queue = queue.PriorityQueue()  # Stores the priority queue of the node, which is where it receives messages
        self.stored_data = []   # Stores the data of the node
        self.successor_list = [None] * r   # Stores the next r successors of the node

    def join_network(self):
        join_lock.acquire()    # Acquire join lock so that only one join happens at a time
        time.sleep(1)
        time1 = time.time()  # Used to calculate time taken for join
        self.id = random.randint(0, len(queues) - 1)  # Take random id
        while self.id in used_ids:  # If id is taken, take another one until it's not
            self.id = random.randint(0, id_limit)

        print(str(self.id) + " gets lock\n")

        used_ids.append(self.id)  # Add id to used id list
        queues[self.id] = self.queue  # Set your queue in the global queue list so that you can get messaged
        if len(used_ids) == 1:  # If you are the first and only node
            self.successor = self.id
            self.predecessor = self.id
        else:
            message = ("find_successor", self.id, self.id)  # Ready the message for find successor with your id
            queues[used_ids[random.randint(0, len(used_ids) - 2)]].put((10, message))  # Put it in a random queue
            reply = self.queue.get()  # Receive a reply
            if reply[1][0] == "find_successor_r":  # If the reply was the result of a find successor
                self.successor = reply[1][1]  # Set successor as it's result
                self.notify()  # Run notify function
            else:
                print("Join failed: ")
                print(reply)
        print(str(self.id) + " releases lock, join time: " + str(time.time() - time1) + "\n")
        join_lock.release()  # Release join lock

    def find_successor(self, _id, receiver):
        print('I am ' + str(self.id) + ' checking successor for ' + str(_id))
        if self.successor_list[r - 1] is not None and id_check(self.id, _id, self.successor):  # If id in question is between yours and your successors and successor list exists
            fails = 0  # Count failed times that tried to successor
            temp_successor = self.successor  # Used to store next successor in successor list
            while True:  # Until alive successor is found
                alive = self.check_if_alive(temp_successor)  # Check if successor is active
                if alive:  # If active, continue with find successor
                    break
                temp_successor = self.successor_list[fails]  # If not, store next successor in successor list
                fails += 1  # And increase fail counter
                if fails > r:  # If fail counter > than size of successor list
                    while True:
                        print("TOTAL FAILURE!!!!!!!!!! Node " + str(self.id) + " is out of node ring")
                        time.sleep(100)
                self.successor = temp_successor  # Update your own successor
            print('Successor found: ' + str(self.successor))
            message = ("find_successor_r", self.successor)  # Ready message with the result
            queues[receiver].put((9, message))  # Put the reply message in the receiver's queue
        elif id_check(self.id, _id, self.successor):  # If id in question is between yours and your successors
            print('Successor found: ' + str(self.successor))
            message = ("find_successor_r", self.successor)  # Ready message with the result
            queues[receiver].put((9, message))  # Put the reply message in the receiver's queue
        elif self.finger_table[m - 1] is not None:  # If your finger table is set
            flag_fs = False  # Flag for loop
            last_finger = self.successor  # Last finger checked, starts as successor
            alive = True
            for finger in self.finger_table:
                if id_check(self.id, _id, finger):  # If id in question is between yours and your finger's
                    alive = self.check_if_alive(last_finger)  # Check if successor of finger is alive
                    if not alive:  # If not, don't use that finger
                        continue
                    message = ("find_successor", _id, receiver)
                    queues[last_finger].put((10, message))  # Forward find successor to the first node before the finger
                    flag_fs = True
                    break
                last_finger = finger
            if not flag_fs:  # If id not between you and any fingers
                alive = self.check_if_alive(self.finger_table[m - 1])  # Check if the biggest finger successor is alive
                if not alive:  # If not, remove the biggest finger from your table so that it updates again
                    self.finger_table[m-1] = None
                    message = ("find_successor", _id, receiver)
                    queues[self.id].put((10, message))  # Forward find successor back to yourself but don't use fingers this time
                if alive:  # If alive, forward find successor
                    message = ("find_successor", _id, receiver)
                    queues[self.finger_table[m - 1]].put((10, message))  # Forward find successor to the furthest finger
        else:  # If finger table is not set
            fails = 0  # Code under here works the same way with as described above
            temp_successor = self.successor
            if self.successor_list[r - 1] is not None:
                while True:
                    alive = self.check_if_alive(temp_successor)
                    if alive:
                        break
                    temp_successor = self.successor_list[fails]
                    fails += 1
                if fails > r:
                    while True:
                        print("TOTAL FAILURE!!!!!!!!!! Node " + str(self.id) + " is out of node ring")
                        time.sleep(100)
                self.successor = temp_successor
            print("Passing find successor to node: " + str(temp_successor))
            message = ("find_successor", _id, receiver)
            queues[temp_successor].put((10, message))  # Pass the task to your successor

    def notify(self):  # Used to notify
        message = ("check_predecessor", self.id)
        queues[self.successor].put((8, message))  # Notify your successor to check its predecessor

    def check_predecessor(self, _id):  # Used to check your predecessor
        print("I am " + str(self.id) + " checking for predecessor")
        if self.predecessor is None:  # If predecessor doesn't exist
            self.predecessor = _id
        elif id_check(self.predecessor, _id, self.id):  # If id between your predecessor's and yours
            message = ("stabilize", "Nothing")
            queues[self.predecessor].put((7, message))  # Tell your old predecessor to stabilize
            self.predecessor = _id  # Replace old predecessor with the correct new one
        print("Oh, " + str(self.id) + " predecessor is " + str(self.predecessor))

    def stabilize(self):
        message = ("give_predecessor", self.id)
        queues[self.successor].put((6, message))  # Ask your successor for its predecessor
        buffer = []  # Buffer to store non-critical requests
        flag = True  # Flag for loop
        while flag:
            reply = self.queue.get()
            if reply[1][0] == "give_predecessor_r":
                if id_check(self.id, reply[1][1], self.successor):  # If successors predecessor is the correct successor
                    self.successor = reply[1][1]  # Replace old successor with new one
                    self.notify()  # Notify new successor of your existence
                    print("Oh, " + str(self.id) + " successor is " + str(self.successor))
                flag = False
            elif reply[1][0] == "give_predecessor":  # Respond to only these request to not get stuck
                self.give_predecessor(reply[1][1])
            elif reply[1][0] == "find_successor":  # Respond to only these request to not get stuck
                print("Finding successor inside find_data_range")
                self.find_successor(reply[1][1], reply[1][2])
            elif reply[1][0] == "ping":
                self.ping(reply[1][1])
            else:  # Store all other requests in a temporary buffer
                buffer.append(reply)
        for x in buffer:  # Put requests in the temporary buffer back in the queue after you're done
            self.queue.put(x)

    def give_predecessor(self, receiver):  # Give your predecessor to whoever asks
        message = ("give_predecessor_r", self.predecessor)
        queues[receiver].put((5, message))

    def find_data_range(self, data, usage):
        self.find_successor(data[0], self.id)  # Find the successor of the hashed data key
        buffer = []
        flag = True
        while flag:
            reply = self.queue.get()
            if reply[1][0] == "find_successor_r":  # If the find successor result is received
                message = ("find_data_owner", data, usage)
                queues[reply[1][1]].put((3, message))  # Forward next function to the successor of the key
                flag = False
            elif reply[1][0] == "find_successor":  # Respond to only these request to not get stuck
                print("Finding successor inside find_data_range")
                self.find_successor(reply[1][1], reply[1][2])
            elif reply[1][0] == "ping":
                self.ping(reply[1][1])
            else:  # Store all other requests in a temporary buffer
                buffer.append(reply)
        for x in buffer:  # Put requests in the temporary buffer back in the queue after you're done
            self.queue.put(x)

    def find_data_owner(self, data, usage):
        belongs = data_id_check(self.predecessor, data[0], self.id)  # If the key is closer to you or your predecessor
        if not belongs:  # If key is closer to your predecessor
            message = ("manipulate_data", data, usage)
            queues[self.predecessor].put((1, message))  # Message predecessor to handle his data
        if belongs:  # If key is closer to you
            message = ("manipulate_data", data, usage)
            queues[self.id].put((1, message))  # Message yourself to handle your data

    def manipulate_data(self, data, usage):
        if usage == "insert":  # If data is to be inserted
            exists = False
            for z in self.stored_data:  # If the data exists already
                if data[1] == z[1]:
                    exists = True
            if exists:
                print("Data: " + str(data) + " already exists in node " + str(self.id))
            else:  # If it doesn't, store it
                self.stored_data.append(data)
                print("Data: " + str(data) + " inserted in node " + str(self.id))
        elif usage == "update":   # If data is to be updated
            exists = False
            for z in self.stored_data:   # If the data exists already
                if data[1] == z[1]:
                    self.stored_data.remove(z)  # Delete old
                    self.stored_data.append(data)  # Add new
                    exists = True
            if exists:
                print("Data: " + str(data) + " updated in node " + str(self.id))
            else:
                self.stored_data.append(data)
                print("Data: " + str(data) + " doesn't exist in any node ")
        elif usage == "lookup":   # If data is to be looked up
            for z in self.stored_data:
                if data[1] == z[1]:   # If the data exists, return it
                    print("Data: " + str(z) + " found in node " + str(self.id))
                    break
        elif usage == "delete":   # If data is to be deleted
            for z in self.stored_data:   # If the data exists
                if data[1] == z[1]:
                    self.stored_data.remove(z)  # Delete it
                    print("Data: " + str(z) + " deleted in node " + str(self.id))
                    break
        query_time.put(time.time())

    def update_fingers(self):
        time1 = time.time()
        buffer = []  # Works like in stabilize function
        for i in range(0, m):
            flag = True
            j = (self.id + pow(2, i)) % id_limit  # Calculates finger length
            message = ("find_successor", j, self.id)
            print("Finding finger: " + str(j))
            if i == 0:  # For the first finger
                queues[self.successor].put((10, message))  # Forward find successor to successor
            else:  # For all others
                queues[self.finger_table[i - 1]].put((10, message))  # Forward find successor to last set finger
            while flag:
                reply = self.queue.get()
                if reply[1][0] == "find_successor_r":  # If the find successor result is received
                    self.finger_table[i] = reply[1][1]  # Store finger successor
                    flag = False
                elif reply[1][0] == "find_successor":
                    self.find_successor(reply[1][1], reply[1][2])
                elif reply[1][0] == "ping":
                    self.ping(reply[1][1])
                else:
                    buffer.append(reply)
        for x in buffer:
            self.queue.put(x)
        time2 = time.time()
        query_time.put(time2)
        print("Finger Table of node: " + str(self.id) + " with time: " + str(time2 - time1))
        print(self.finger_table)

    def update_successors(self):
        buffer = []  # Works like in stabilize function
        time1 = time.time()
        for i in range(0, r):
            flag = True
            if i == 0:
                message = ("find_successor", self.successor, self.id)
                queues[self.successor].put((10, message))  # Pass find successor to successor
            else:
                message = ("find_successor", self.successor_list[i - 1], self.id)
                queues[self.successor_list[i - 1]].put((10, message))  # Pass find successor to the furthest successor
            print("Finding successor for successor list of node: " + str(self.id) + " for index " + str(i))
            while flag:
                reply = self.queue.get()
                if reply[1][0] == "find_successor_r":  # If the find successor result is received
                    self.successor_list[i] = reply[1][1]  # Store finger successor
                    flag = False
                elif reply[1][0] == "find_successor":
                    self.find_successor(reply[1][1], reply[1][2])
                elif reply[1][0] == "ping":
                    self.ping(reply[1][1])
                else:
                    buffer.append(reply)
        time2 = time.time()
        query_time.put(time2)
        print("Successor list of node: " + str(self.id) + " with time: " + str(time2 - time1))
        print(self.successor_list)
        for x in buffer:
            self.queue.put(x)

    def node_remove(self, _id, starting_node=None):  # Sends remove requests to node with _id
        while True:
            if self.id == _id:
                print("Node {} was removed!".format(_id))
                self.queue = queue.PriorityQueue()
                del self
                query_time.put(time.time())
                return True
            elif starting_node == self.id and self.id != _id:
                print("Node does not exist for removal")
                return False
            else:
                if starting_node is None:
                    starting_node = self.id
                message = ("node_remove", _id, starting_node)
                queues[self.successor].put((16, message))  # Pass the task to your successor
                return False


    def check_if_alive(self, _id):  # Check if a node is alive
        message = ("ping", self.id)
        queues[_id].put((1, message))  # Ping the node
        buffer = []
        timer = time.time() + 30
        flag = False
        while True:
            if self.queue.qsize() > 0:
                reply = self.queue.get()
                if reply[1][0] == "pong":  # If the ping result is received, return true
                    flag = True
                    break
                elif reply[1][0] == "ping":
                    self.ping(reply[1][1])
                elif reply[1][0] == "find_successor":
                    self.find_successor(reply[1][1], reply[1][2])
                else:
                    buffer.append(reply)
            if timer < time.time() + 15:  # If time runs out, return false
                break
        for x in buffer:
            self.queue.put(x)
        return flag

    def ping(self, receiver):  # Used to ping a node
        message = ("pong", "Nothing")
        queues[receiver].put((1, message))


def data_id_check(start, key, end):  # Checks if key is closer to start or end
    if start <= end:
        if key - start < end - key:
            return False
        else:
            return True
    elif end < start:
        if key <= end:
            end = end + id_limit
            key = key + id_limit
        else:
            end = end + id_limit
        if key - start < end - key:
            return False
        else:
            return True

    return


def id_check(start, _id, end):  # Checks if an id is between two others, clock-wise
    if start == end:
        return True
    elif start < end:
        if start <= _id < end:
            return True
    elif end < start:
        if _id < end or start <= _id:
            return True
    return False


def run():
    node = Node()
    node_list.append(node)
    node.join_network()
    time.sleep(5)
    timer_f = 0
    timer_s = 0
    deleted = False
    while enabled.is_set() and deleted is False:  # If enabled. When disabled, shuts down
        while freeze.is_set():  # If freeze is set
            time.sleep(10)
        if node.queue.qsize() > 0:  # If queue isn't empty
            request = node.queue.get()  # Receive request and find what type of request it is, then run its function
            if request[1][0] == "find_successor":
                node.find_successor(request[1][1], request[1][2])
            if request[1][0] == "stabilize":
                node.stabilize()
            if request[1][0] == "check_predecessor":
                node.check_predecessor(request[1][1])
            if request[1][0] == "give_predecessor":
                node.give_predecessor(request[1][1])
            if request[1][0] == "find_data_range":
                node.find_data_range(request[1][1], request[1][2])
            if request[1][0] == "find_data_owner":
                node.find_data_owner(request[1][1], request[1][2])
            if request[1][0] == "manipulate_data":
                node.manipulate_data(request[1][1], request[1][2])
            if request[1][0] == "node_remove":
                deleted = node.node_remove(request[1][1], request[1][2])
            if request[1][0] == "ping":
                node.ping(request[1][1])
        if timer_f == 0:  # If timer for finger update isn't set, set it
            timer_f = time.time() + random.uniform(10 * number_of_nodes, 10 * number_of_nodes + 300)
        if (time.time() > timer_f or node.finger_table[r - 1] is None) and not update_fingers.is_set():  # If timer is reached and finger update is allowed
            update_fingers.set()  # Forbid update fingers for others
            node.update_fingers()  # Update fingers
            update_fingers.clear()  # Allow update fingers for others
            timer_f = 0  # Reset timer
        if timer_s == 0:  # If timer for successor list update isn't set, set it
            timer_s = time.time() + random.uniform(20 * number_of_nodes, 20 * number_of_nodes + 400)
        if (time.time() > timer_s or node.successor_list[r - 1] is None) and not update_successors.is_set():  # If timer is reached and finger update is allowed
            update_successors.set()  # Forbid update fingers for others
            node.update_successors()  # Update fingers
            update_successors.clear()  # Allow update fingers for others
            timer_s = 0  # Reset timer

    print(
        "Pre: " + str(node.predecessor) + " Me: " + str(node.id) + " Suc: " + str(node.successor) + " F_Table: " + str(
            node.finger_table))

