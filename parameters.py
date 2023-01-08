import threading
import queue
used_ids = []  # To store ids in use
thread_list = []  # To store all active threads
node_list = []  # To store all active nodes
number_of_nodes = 8  # Maximum number of nodes
id_limit = 63  # Maximum value of id that a node can have
m = 5  # Number of fingers
r = 3  # Number of successors
queues = {}  # Global queue list, stores the queues of all reachable nodes
# threads = []  # Stores all threads
enabled = threading.Event()  # If enabled, thread loops

join_lock = threading.Lock()  # Used so that nodes join one by one
update_fingers = threading.Event()  # Used to allow or forbid finger updates
update_successors = threading.Event()  # Used to allow or forbid successor list updates
freeze = threading.Event()  # Used to temporarily stop threads/nodes
query_time = queue.Queue()  # Queue to message time taken for a query
