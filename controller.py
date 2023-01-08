import random
import time

from parameters import *
import thread_script as ts
import parseTesting as pt

stop = False

print("Enter 1 for auto-setup, 2 for manual setup")
while True:
    inp = int(input())
    if inp == 1:
        print("Auto-setup with " + str(number_of_nodes) + " nodes, id limit: " + str(id_limit) + " and m: " + str(m))
        break
    elif inp == 2:
        print("Manual-setup, choose id_limit: ")
        inp = int(input())
        id_limit = inp
        print("Choose number of nodes: ")
        inp = int(input())
        number_of_nodes = inp
        print("Choose m: ")
        inp = int(input())
        m = inp
        print("Choose r: ")
        inp = int(input())
        r = inp
        break
    else:
        print("Wrong input, retry")

print("Setting up...")
time.sleep(1)
i = 0
enabled.set()  # Enable threads
while i < id_limit:  # Set up global queues dictionary
    queues[i] = None
    i += 1

update_fingers.set()  # Disable finger updates until all nodes join
update_successors.set()  # Disable update successors

time1 = time.time()

for i in range(0, number_of_nodes):  # Create all threads
    print(i)
    t = threading.Thread(target=ts.run)
    t.start()
    thread_list.append(t)

while len(used_ids) < number_of_nodes:  # Wait until all threads but last have joined
    time.sleep(15)

time.sleep(15)  # Wait for last thread

while query_time.qsize() > 1:
    trash = query_time.get()

time2 = time.time()

print("Total join time: " + str(time2 - time1))

update_fingers.clear()  # Allow finger updates

time1 = time.time()

time.sleep(14 * number_of_nodes)  # Give time for finger updates

while update_fingers.is_set():  # Wait until not used disable finder updates
    time.sleep(1)
update_fingers.set()

while query_time.qsize() > 1:
    trash = query_time.get()

time2 = query_time.get()

print("Total finger table update time: " + str(time2 - time1))

print("")

update_successors.clear()

time1 = time.time()

time.sleep(8 * number_of_nodes)  # Give time for successor updates

while update_successors.is_set():
    time.sleep(1)
update_successors.set()

while query_time.qsize() > 1:
    trash = query_time.get()

time2 = query_time.get()

print("Total successor list update time: " + str(time2 - time1))


data = pt.get_data(0, 10, id_limit + 1)  # Fill nodes with 10 different data
for x in data:
    message = ("find_data_range", x, "insert")
    print(message)
    queues[random.choice(used_ids)].put((15, message))

time.sleep(20)  # Allow time for data to be stored

while query_time.qsize() > 0:
    now = query_time.get()

while stop is False:
    freeze.set()
    time.sleep(2)
    print("The simulation has been frozen.")
    print("Type:\n")
    print("0) Get node info\n")
    print("1) Insert data\n")
    print("2) Delete data\n")
    print("3) Update data\n")
    print("4) Exact match data (lookup)\n")
    print("5) Add nodes\n")
    print("6) Remove node\n")
    print("7) Massive node failure\n")
    print("8) Continue simulation for x seconds\n")
    print("9) End simulation\n")
    inp = int(input())
    if inp == 0:  # Print current node info
        print("Active nodes: " + str(used_ids))
        for temp_node in node_list:
            print("Pre: " + str(temp_node.predecessor) + " Me: " + str(temp_node.id) + " Suc: " + str(temp_node.successor) + " F_Table: " + str(temp_node.finger_table) + " Successor list: " + str(temp_node.successor_list))
            print("Node " + str(temp_node.id) + " data: ")
            for temp_data in temp_node.stored_data:
                print(data)
            print("\n")
    elif inp == 1:  # Insert data
        print("Give data string to insert")
        inp = input()
        tup = pt.data_hash(inp, id_limit)  # Hash the string that the user used as input
        message = ("find_data_range", tup, "insert")  # Prepare message with data and usage type insert
        print(message)
        time1 = time.time()
        queues[random.choice(used_ids)].put((15, message))  # Forward find_data_range to a random node
        freeze.clear()  # Resume simulation
        time2 = query_time.get()  # Wait until completion and get completion time
        print("Time taken until completion: " + str(time2 - time1))  # Calculate completion time
    elif inp == 2:  # Works like insert
        print("Give key string to delete")
        inp = input()
        tup = (pt.GetHashKey(inp, id_limit), pt.GetHashValue(inp))
        message = ("find_data_range", tup, "delete")
        print(message)
        time1 = time.time()
        queues[random.choice(used_ids)].put((15, message))
        freeze.clear()
        time2 = query_time.get()
        print("Time taken until completion: " + str(time2 - time1))
    elif inp == 3: # Works like insert
        print("Give data string to update")
        inp = input()
        tup = pt.data_hash(inp, id_limit)
        message = ("find_data_range", tup, "update")
        print(message)
        time1 = time.time()
        queues[random.choice(used_ids)].put((15, message))
        freeze.clear()
        time2 = query_time.get()
        print("Time taken until completion: " + str(time2 - time1))
    elif inp == 4: # Works like insert
        print("Give key string to match")
        inp = input()
        tup = (pt.GetHashKey(inp, id_limit), pt.GetHashValue(inp))
        message = ("find_data_range", tup, "lookup")
        print(message)
        time1 = time.time()
        queues[random.choice(used_ids)].put((15, message))
        freeze.clear()
        time2 = query_time.get()
        print("Time taken until completion: " + str(time2 - time1))
    elif inp == 5:  # Repeats the actions that were taken at the start of the setup for the new nodes
        print("Add how many nodes:")
        inp = int(input())
        number_of_nodes = number_of_nodes + inp
        for i in range(0, inp):
            print(i)
            t = threading.Thread(target=ts.run)
            t.start()
            thread_list.append(t)
        freeze.clear()
        while len(used_ids) < number_of_nodes:  # Until all threads but last have joined
            time.sleep(15)
        time.sleep(inp * 6)
        update_fingers.clear()
        time.sleep(6 * number_of_nodes)
        while update_fingers.is_set():
            time.sleep(1)
        update_fingers.set()
        update_successors.clear()
        time.sleep(6 * number_of_nodes)
        while update_successors.is_set():
            time.sleep(1)
        update_successors.set()
    elif inp == 6:  # User chooses which node to remove
        print("Current nodes:" + str(used_ids))  # Display ids of all active nodes
        print("Choose which node to remove:")
        inp = int(input())
        message = ("node_remove", inp, None)  # Prepare message with the node id the user chose
        time1 = time.time()
        queues[random.choice(used_ids)].put((15, message))  # Forward node_remove to a random node
        freeze.clear()
        time2 = query_time.get()
        print("Time taken until completion: " + str(time2 - time1))
    elif inp == 7: # Choose how many nodes to randomly remove
        print("Current nodes:" + str(used_ids.sort()))
        print("Choose how many nodes to remove:")
        inp = int(input())
        rand_ids = []
        i = 0
        for x in range(0, inp):  # Choose nodes randomly and remove them from the active list in each step
            rand_ids.append(random.choice(used_ids))
            used_ids.remove(rand_ids[i])
            i += 1
        for x in rand_ids:  # Forward remove request for each node
            message = ("node_remove", x, None)
            queues[random.choice(used_ids)].put((11, message))
        freeze.clear()
        time.sleep(5 * len(rand_ids))
    elif inp == 8:
        print("Continue simulation for how many seconds:")
        inp = int(input())
        freeze.clear()
        time.sleep(inp)
    elif inp == 9:
        stop = True
        update_fingers.set()
        update_successors.set()
        freeze.clear()

enabled.clear()  # Wait until all threads have finished
for t in thread_list:
    t.join()

used_ids.sort()
print(used_ids)
