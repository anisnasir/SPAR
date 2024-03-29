# Include python library for pycassa
import sys
import pycassa
import node
import base64
import uuid

# Connect to Cassandra Instance
from pycassa.index import *
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily


#---------------------------------------------------------------------------------------#
#------------------------Functions------------------------------------------------------#

# Function: Check if user id exists in the table
def node_exists(col_fam, row_key):
	try:
		a = col_fam.get(str(row_key))
		return True
	except:
		#print "Unexpected error in functions.node_exists:", sys.exc_info()[0] # if uncomment, it gets print!
		return False

# Function: Check if edge exists in the table
def edge_exists(col_fam, user1, user2):
	try:
		u_expr = create_index_expression('u',user1)
		v_expr = create_index_expression('v',user2)
		users_clause = create_index_clause([u_expr,v_expr])
		for key, user in col_fam.get_indexed_slices(users_clause):
			return True
		return False
	except:
		print "Unexpected error in functions.edge_exists:", sys.exc_info()[0]
		return False

# Function: Get a UUID - URL safe, Base64
def get_a_Uuid():
    r_uuid = base64.urlsafe_b64encode(uuid.uuid4().bytes)
    return r_uuid.replace('=', '')


# Function: Return the server based on the user id (master)
def getServer(col_fam, user):
	try:
		val = col_fam.get(str(user))
		#print val['server']
		return val['server']
	except:
		return -1

# Function: Return the servers based on the user id (replica)
# Query in Replica 
def get_server_rep(col_fam, id1):
	try:
		node_expr = create_index_expression('node_id',id1)
		node_clause = create_index_clause([node_expr])
		array = []
		for key, server in  col_fam.get_indexed_slices(node_clause):
			array.append(server['server'])
		return array	
	except:
		#print "Unexpected error:", sys.exc_info()[0]
		return -1


# Function: delete a node from replica table
def delete_node_replica(col_fam_replica, id1, server):
	try:
		node_expr = create_index_expression('node_id',id1)
		server_expr = create_index_expression('server',server)
		node_clause = create_index_clause([node_expr,server_expr])
		for key, value in  col_fam_replica.get_indexed_slices(node_clause):
			col_fam_replica.remove(key)
		return True	
	except:
		#print "Unexpected error:", sys.exc_info()[0]
		return False


#---------------------------------------------------------------------------------------#
#----------Insert Functions-------------------------------------------------------------#

#Function: enter Master Master
def insertMM(col_fam_mme, u, v, server):
	try:	
		#always store in sorted order
		if (v < u):
			temp = u
			u=v
			v=temp
		#check if already exists		
		u_expr = create_index_expression('master1', u)
		v_expr = create_index_expression('master2', v)
		server_expr = create_index_expression('server', server)
		clause = create_index_clause([u_expr, v_expr, server_expr])

		flag = False	
		for key, user in col_fam_mme.get_indexed_slices(clause):
			flag = True	
		
		if flag:
			return False
		else:
			col_fam_mme.insert(get_a_Uuid(),{'master1':u,'master2':v,'server':server})
		return True

	except:
		print "Unexpected error in function.insertMM:", sys.exc_info()[0]
		return False

#Function: enter Master Slave
def insertMS(col_fam, u, v, server):
	try:	
		#check if already exists		
		master_expr = create_index_expression('master', u)
		slave_expr = create_index_expression('slave', v)
		server_expr = create_index_expression('server', server)
		clause = create_index_clause([master_expr, slave_expr, server_expr])

		flag = False	
		for key, user in col_fam.get_indexed_slices(clause):
			flag = True

		if flag:
			return False
		else:
			col_fam.insert(get_a_Uuid(),{'master':u,'slave':v,'server':server})
		return True
	except:
		print "Unexpected error in function.insertMS:", sys.exc_info()[0]
		return False

#---------------------------------------------------------------------------------------#
#------------Configurations 1,2,3-------------------------------------------------------#

# Function: Return 1 if nodes are co-located
# master-master, master-slave & vice-versa, slave-slave
def colocated(node1,node2,col_fam_mme,col_fam_mse):
	master1 = node1.getMaster()
	master2 = node2.getMaster()
	
	if master1 == master2:
		insertMM(col_fam_mme, int(node1.getId()),int(node2.getId()),int(master1))
		return True
	
	array1 = node1.getReplicas()
	array2 = node2.getReplicas()
	
	if array1 == -1 or array2 == -1:
		return 0

	# check if replica of node2 exists in server with master1
	for serv in array2:
		if int(master1) == int(serv):
			for serv1 in array1:
				if int(serv1) == int(master2):
					insertMS(col_fam_mse, int(node1.getId()),int(node2.getId()),int(master1))
					insertMS(col_fam_mse, int(node2.getId()),int(node1.getId()),int(master2))				
					return True
	return False

# Function: Configuration 1
# Create slave in the server with other node's master
def config1(node1, node2, col_fam_replica, col_fam_mse, move_flag):
	added_replicas=0
	
	master1 = node1.getMaster()
	master2 = node2.getMaster()

	array1 = node1.getReplicas()
	array2 = node2.getReplicas()
	flag = True
	for serv in array2:
		if int(master1) == int(serv):
			flag = False
			break 
	if flag:
		if move_flag == 0:
			added_replicas+=1
		else:
			col_fam_replica.insert(get_a_Uuid(),{'node_id':node2.getId(),'server':master1})
	flag = True
	for serv in array1:
		if int(serv) == int(master2):
			flag = False
			break
	if flag:
		if move_flag == 0:
			added_replicas+=1
		else:
			col_fam_replica.insert(get_a_Uuid(),{'node_id':node1.getId(),'server':master2})
	
	if move_flag == 1:
		insertMS(col_fam_mse, int(node1.getId()),int(node2.getId()),int(master1))
		insertMS(col_fam_mse, int(node2.getId()),int(node1.getId()),int(master2))

	return added_replicas

# Function: Search for replicas of master-neighbours in the new server
# If node1-master has masters-neighbours, check if their replicas exist in server2
# If not --> Create replicas-neighbours in the server2
def find_master_replicas(node_id, node_server1, node_server2, col_fam_mme, col_fam_mse, col_fam_replica, str_master1, str_master2, move_flag):
	counter=0
	added_replicas=0
	#print "move_flag ",move_flag
	try:
	# Search in Master_Master_Edge if node1-master has masters-neigbours
		master_expr = create_index_expression('%s'%str_master1, node_id)
		server1_expr = create_index_expression('server', node_server1)
		clause1 = create_index_clause([master_expr, server1_expr])			
		for key1 , mms in  col_fam_mme.get_indexed_slices(clause1):
			#print "found master master ",node_id," and ", mms['%s'%str_master2], " on server ",node_server1
	# Search in Replica if slave-neighbours exist in the server2 --> keep a counter
			slave_expr = create_index_expression('node_id', mms['%s'%str_master2])
			server2_expr = create_index_expression('server', node_server2)
			clause2 = create_index_clause([slave_expr, server2_expr])
			for key2, server in col_fam_replica.get_indexed_slices(clause2):
				#print "found replica of ",mms['%s'%str_master2]," on server ",node_server2
				counter+=1
	
			if counter==0:
				#master-neighbour has NOT slave-replica in server2
				if move_flag == 0:
					added_replicas+=1
				else:
					
					#print node_id,"is moving from ",node_server1," to ",node_server2
					col_fam_replica.insert(get_a_Uuid(),{'node_id':int(mms['%s'%str_master2]),'server':node_server2})
					insertMS(col_fam_mse, int(node_id), int(mms['%s'%str_master2]), int(node_server2))
			else:
				counter=0
				if move_flag:
					col_fam_mme.remove(key1)
					insertMS(col_fam_mse, int(node_id), int(mms['%s'%str_master2]), int(node_server2))
					col_fam_replica.insert(get_a_Uuid(), {'node_id':node_id, 'server':node_server1})
		return added_replicas
	except:
		print "Unexpected error in functions.find_master_replicas:", sys.exc_info()[0]
		return 0

# Function: Search for replicas of slave-neighbours in the new server
def find_slave_replicas(node1, node2, node_id, node_server1, node_server2, col_fam_mme, col_fam_mse, col_fam_master, col_fam_replica, total_replicas, move_flag,total_servers):
	counter = 0
	added_replicas = 0
	
	# If node1-master has slaves-neighbours, 
	# check if: (a) the same slaves-neighbours exist in server 2
	#           (b) the masters-neighbours exist in server2
	try:
		# Search in Master_Slave_Edge if node1-master has neighbours-slaves
		# For each slave neighbour find his master location
		# If his master is located in server2 --> find if there is a slave in server2					
		master_expr = create_index_expression('master',node_id)
		server1_expr = create_index_expression('server',node_server1)
		clause1 = create_index_clause([master_expr,server1_expr])
		for key1, mss in col_fam_mse.get_indexed_slices(clause1):
			slave_server = col_fam_master.get(str(mss['slave']))
			
			if not slave_server == node_server2:
				slave_expr = create_index_expression('node_id', mss['slave'])
				server2_expr = create_index_expression('server', node_server2)					
				clause2 = create_index_clause([slave_expr, server2_expr])
				for key2, server in col_fam_replica.get_indexed_slices(clause2): 					
					counter+=1		
				if counter==0:
					if move_flag == 0:
						added_replicas+=1
					else:
						col_fam_replica.insert(get_a_Uuid(), {'node_id':mss['slave'], 'server':node_server2})
						insertMS(col_fam_mse, node_id, mss['slave'], node_server2)
				else:
					counter=0
					if move_flag == 1:
						insertMS(col_fam_mse, node_id, mss['slave'], node_server2)
			else:
				if move_flag == 1:
					insertMM(col_fam_master,node_id,mss['slave'], node_server2)
			
			# Check if the replicas need to be kept in the initial server
			counter=0
			slave_expr1 = create_index_expression('slave',mss['slave'])
			server3_expr = create_index_expression('server',node_server1)
			clause3 = create_index_clause([slave_expr1, server3_expr])
			for key2, mss2 in col_fam_mse.get_indexed_slices(clause3):
				if not int(mss2['master']) == node_id:
					counter+=1
				else:
					if move_flag == 1:
						col_fam_mse.remove(key2)
			if counter==0:
				new_replica_count = get_server_rep(col_fam_replica,int(mss['slave']))
				if (len(new_replica_count) > total_replicas):
					if move_flag == 0:
						added_replicas-=1
					else:
						delete_node_replica(col_fam_replica,int(mss['slave']) , node_server1)
			else:
				counter=0

		
		return added_replicas
	except:
		print "Unexpected error in functions.find_slave_replicas:", sys.exc_info()[0]
		return 0

	

# Function: Configurations 2 and 3
# Move the master of node1 in the server of master-node2
def config2_3(node1, node2, col_fam_mme, col_fam_replica, col_fam_mse, col_fam_master, str_master1, str_master2, total_replicas, move_flag,total_servers):
	added_replicas=0

	node_id = node1.getId()		#master1
	node_server1 = node1.getMaster()
	node_server2 = node2.getMaster()

	added_replicas+=find_master_replicas(node_id, node_server1, node_server2, col_fam_mme, col_fam_mse, col_fam_replica, str_master1, str_master2, move_flag)
	# The same for master 2
	added_replicas+=find_master_replicas(node_id, node_server1, node_server2 , col_fam_mme, col_fam_mse, col_fam_replica, str_master2, str_master1, move_flag)
	added_replicas+=find_slave_replicas(node1, node2, node_id, node_server1, node_server2, col_fam_mme, col_fam_mse, col_fam_master, col_fam_replica, total_replicas, move_flag,total_servers)
	
	if move_flag == 1:
		col_fam_master.remove(str(node_id))
		node_replicas = get_server_rep(col_fam_replica,node_id)
		if node_server2 in node_replicas:
			servers = range (total_servers)
			delete_node_replica(col_fam_replica,node_id,node_server2)
			new_replicas = set(servers) - set(node_replicas)
			new = list(new_replicas)
			if len(new) >0:
				col_fam_replica.insert(get_a_Uuid(), {'node_id':node_id, 'server':new[0]})
			col_fam_master.insert(str(node_id),{'server':node_server2})
		else:
			col_fam_master.insert(str(node_id),{'server':node_server2})
			insertMM(col_fam_mme, int(node_id),int(node2.getId()),int(node_server2))
	
	return added_replicas
#-----------EOF----------------------------------------------------------------------------#
#---------------------------------------------------------------------------------------#
