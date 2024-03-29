# Include python library for pycassa
import sys
import functions
import node
import pycassa

from pycassa.index import *

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

# Empty tables
col_fam_master.truncate()
col_fam_replica.truncate()
col_fam_edge.truncate()
col_fam_mme.truncate()
col_fam_mse.truncate()

# Input: Number of servers & number of replicas (K-redundancy)
total_servers =4
total_replicas = 2

# Variables
server_id = 0
#num_replica = 0
replica_id =0

# Initialization of the table with masters in each server
server_master_list = [0]*total_servers 

if len(sys.argv) != 2:
	raise sys.exit("Number of arguments should be 2. Give a dataset with nodes. \nExiting...")
	
# Read the nodes file and store the nodes 
file_name = str(sys.argv[1])
nodes_array=[]
try:
	with open(file_name, 'r') as f:
		for line in f:	
			# Ignore comment lines
			if not line.startswith('#'):
				val = int(line)
				node_exists = functions.node_exists(col_fam_master, val)
				# If node does not exist, ADD node in the tables of masters and replicas	
				if not node_exists:
					col_fam_master.insert(str(val),{'server':server_id})	
					server_master_list[server_id] += 1
					server_id = (server_id+1) % total_servers
					replica_id = server_id
					for value1 in range(total_replicas):
						col_fam_replica.insert(functions.get_a_Uuid(),{'node_id':val,'server':replica_id})
						#num_replica += 1
						replica_id = (replica_id+1) % total_servers
except:
	print "Unexpected error in insert_nodes.py:", sys.exc_info()[0]

print "Number of masters in each server",server_master_list

