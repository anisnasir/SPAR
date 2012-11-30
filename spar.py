# Include python library for pycassa
import sys
import functions
import node
import pycassa

from pycassa.index import *
from datetime import datetime

# Connect to Cassandra Instance
from pycassa.pool import ConnectionPool
pool = ConnectionPool('OSN')
#pool = ConnectionPool('OSN', ['localhost:9160'])

from pycassa.columnfamily import ColumnFamily
col_fam_master = pycassa.ColumnFamily(pool, 'Master')
col_fam_replica = pycassa.ColumnFamily(pool, 'Replica')
col_fam_edge = pycassa.ColumnFamily(pool, 'Edge')
col_fam_mme = pycassa.ColumnFamily(pool, 'Master_Master_Edge')
col_fam_mse = pycassa.ColumnFamily(pool, 'Master_Slave_Edge')

# Input: Number of servers & number of replicas (K-redundancy)
total_servers =4
total_replicas = 2

# Variables
server_id = 0
num_replica = 0
replica_id =0

total_replicas_needed =0
edge_count =0

if len(sys.argv) != 2:
	raise sys.exit("Number of arguments should be 2. Give a dataset with edges. \nExiting...")

t1 = datetime.now()
# Read the edges file and execute SPAR algorithm
file_name = str(sys.argv[1])
with open(file_name, 'r') as f:
	for line in f:
		edge_exists = False
		# nodes hold the 2 end-nodes of the edge
    		nodes = [int(x) for x in line.split()]
		print "(%d,%d)" % (nodes[0], nodes[1])
		added_replicas1=0
		added_replicas2=0
		added_replicas3=0

		user1 = node.node(nodes[0], functions.getServer(col_fam_master,nodes[0]), functions.get_server_rep(col_fam_replica, nodes[0]))
		user2 = node.node(nodes[1], functions.getServer(col_fam_master,nodes[1]), functions.get_server_rep(col_fam_replica, nodes[1]))
		"""
		for i in user1.replicas:
			print i
		for i in user2.replicas:
			print i
		"""
		if nodes[0] < nodes[1]:
			if functions.edge_exists(col_fam_edge, nodes[0], nodes[1]) == False:
				col_fam_edge.insert(functions.get_a_Uuid(), {'u':nodes[0],'v':nodes[1]})
				edge_count += 1
			else:
				edge_exists = True
		else:
			if functions.edge_exists(col_fam_edge, nodes[1], nodes[0]) == False:
				col_fam_edge.insert(functions.get_a_Uuid(), {'u':nodes[1],'v':nodes[0]})
				edge_count +=1
			else:
				edge_exists = True

		#if edge_exists == False:
		if not edge_exists:
			# Check if masters/replicas of both nodes are co-located
			flag_col = functions.colocated(user1, user2, col_fam_mme, col_fam_mse)		
			#print "%s --> %d" % (line, flag_col)
			#print "nodes[%d,%d] in servers[%d,%d]" % (user1.getId(), user2.getId(), user1.getMaster(), user2.getMaster())
			if flag_col == 1:
				print "--> Co-located!"
			move_flag=0
			if flag_col == 0:
				# Configuration 1
				added_replicas1 += functions.config1(user1, user2, col_fam_replica, col_fam_mse, move_flag)
	
				# Configuration 2
				added_replicas2 += functions.config2_3(user1, user2, col_fam_mme, col_fam_replica, col_fam_mse, col_fam_master, 'master1', 'master2', total_replicas, move_flag)
				
				# Configuration 3
				added_replicas3 += functions.config2_3(user2, user1, col_fam_mme, col_fam_replica, col_fam_mse, col_fam_master, 'master2', 'master1', total_replicas, move_flag)
				
				print "replicas(%d,%d,%d)" %(added_replicas1,added_replicas2,added_replicas3)
				# Find minimum amount of added replicas
				replicas = [added_replicas1,added_replicas2,added_replicas3]						
				value =  replicas.index(min(replicas))
				move_flag=1
				if(value == 0): # Configuration 1
					total_replicas_needed+= replicas[0]
					functions.config1(user1, user2, col_fam_replica, col_fam_mse, move_flag)
				elif(value == 1): # Configuration 2
					total_replicas_needed+= replicas[1]
					functions.config2_3(user1, user2, col_fam_mme, col_fam_replica, col_fam_mse, col_fam_master, 'master1', 'master2', total_replicas, move_flag)
				else:		# Configuration 3
					total_replicas_needed+= replicas[2]
					functions.config2_3(user2, user1, col_fam_mme, col_fam_replica, col_fam_mse, col_fam_master, 'master2', 'master1', total_replicas, move_flag)

t2 = datetime.now()

delta = t2 - t1		# The result is a timedelta object
print "total edges: %d" %edge_count 
print "total time elapsed: ",str(delta)[:-4]
print "total replicas movement: %d" %total_replicas_needed 


