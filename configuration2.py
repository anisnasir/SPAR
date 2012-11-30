def config2(node1, node2, col_fam_mme, col_fam_replica, col_fam_mse, col_fam_master):
	added_replicas=0
	counter=0

	node_id1 = node1.getId()		#master1
	server2 = node2.getMaster()

	# If node1-master has masters-neighbours, check if their replicas exist in server2
	# If not --> Create replicas-neighbours in the server2
	try:
	# Search in Master_Master_Edge if node1-master has masters-neigbours
		master_expr1 = create_index_expression('master1', node_id)
		clause1 = create_index_clause([master_expr1])			
		for key1, master2 in  col_fam_mme.get_indexed_slices(clause1):
			print "-----master1=%s has masters-neighbours" %node_id
			#print type(master2['master2'])
			#print type(server2)
			
			slave_expr1 = create_index_expression("node_id", master2['master2'])
			server_expr1 = create_index_expression("server", server2)
			clause2 = create_index_clause([slave_expr1, server_expr1])

	# Search in Replica if slave-neighbours exist in the server2 --> keep a counter
			for key2, server in col_fam_replica.get_indexed_slices(clause2):
				print "-----master-neighbour=%s has slave-replica in server2=%s" %(master2['master2'],server2)
				counter+=1	
			if counter==0:
				print "-----master-neighbour=%s has NOT slave-replica in server2=%s" %(master2['master2'],server2)
				added_replicas+=1
				#node2.incAmount()		# K - redundancy
		print "-----Node : %d moving to server: %d" %(node_id,server2)
		return added_replicas
	except:
		print "Unexpected error:", sys.exc_info()[0]
		return 0

	# If node1-master has slaves-neighbours, 
	# check if the same slaves-neighbours or the masters-neighbours exist in server2
	
	# Search in Master_Slave_Edge if node1-master has neighbours-slaves
	master_expr2 = create_index_expression('master',master)
	clause3 = create_index_clause([master_expr2])

	for key, master2, server in  col_fam_mse.get_indexed_slices(clause3):
		# TODO: if not null
		slave_expr2 = create_index_expression("node_id",master2)
		clause4 = create_index_clause([slave_expr2])
	# Search in Master if master-neighbours exist in the server2
		for server in col_fam_master.get_indexed_slices(clause4):
		# TODO: if not null
			counter+=1	
		if counter==0:
			added_replicas+=1
			replica[master2]+=1;		# K - redundancy
	# TODO: Search in ____ if slaves-neighbours exist in the server2

	return added_replicas	

			
