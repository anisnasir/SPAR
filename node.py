class node:
	def __init__(self,node_id,master,replicas):
		self.node_id = node_id        	
		self.master = master
        	self.replicas = replicas

	def getId(self):
		return self.node_id
	def getMaster(self):
		return self.master
	def getReplicas(self):
		return self.replicas
	def getSelf(self,node_id):
		return self
