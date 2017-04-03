# Distributed Graphs

##Getting up and running bits 

### Json Gen

   Int postNum <- reads in from file so always unique number  
   gen vertex/edge add, update, removal with overload to specify src/dstID

</br>
</br>
</br>

### Consumer test

Create actor factory  
Create list of children (graph partitions)  
Create list of graph managers (passing children)  
Start data stream, passing commands to graph managers in round robin

##Graph entities

###Entity

Takes creation message and initial state (This is done because an entity can be created by a delete (if things in wrong order)

Property map - [“Key”, Property Obj]  
Previous state list [msgID, T/F]  
remove List [msgID, (T/F, “Value)] ------- keeps track of all removes that have been sent to vertex, this is so we can pass them to new properties, making sure the information inside 	properties is always the same (for filtering windows of time etc.)

**Revive Block**  
If received add message is the most up to date, put at front of the previous state list, otherwise search through the list until the correct position has been found

**Kill Block**  
Same as revive, but with delete messages. This also updates the remove list to include the new remove command and sends kill messages to all properties it stores so their inner lists are up to date.

**Property Block**  
Overwrites the bass apply function so can do Entity(“prop key”) to retrieve property from inner list.
  
Overwrites + function to add properties - if property exists in list, will call update on property passing variables if doesn’t exist, will add new property to map passing all info and the remove list.

**Print Block**  
Can print (return string) of full history/current value of entity + its properties. 

</br>
</br>
</br>



###Vertex
Takes creation msg ID, the vertex ID and initial value - extends Entity passing needed vars. 
	
Vertex Id int  
Associated Edges Set(Int, Int) - tracks the edges associated with this vertex so that if it is removed they can also be affected
	
**Associated Edge Block**	
Contains functions to add, remove and check the contents of teh associated edge set.
		
</br>
</br>
</br>


###Edge/Remote edge 
	
Takes creation message ID, Src/Dst ID and the inital value 
	
SrcID/DstID Int
	
Alternative subclass Remote edge used to check if edge spans partitions - contains enum to specifiy if the partition holds the src or dst node. Contains id of partition also holding edge 

</br>
</br>
</br>

###Property 

Takes the creation messge ID, its Key and starting value and a remove list   
	Cannot be created via a remove because properties are only removed when the equivilent vertex is - remove list keeps all previous removes in case of out of order messaging, passed to any new property
	
intially create Int - intial message ID   
key Int  
previous state List [Int, (T/F, Value)] stores the message id of changes, if the property was alive at that point and if so what was its value. 
	
**Update Block**  
Same as Revive block in Entity - takes msgId and new value, slotting it into previous value list
	
**Kill Block**  
Same as revive, but only takes msgID, setting the value to empty string
		
**Print block**  
Overwrite toString to return the full property history in print friendly manner

##Graph Actors

###Graph manager

Contains Case classes for all message types:
	
		VERTEX MESSAGES
		VertexAdd(msgId,srcId)
		VertexAddWithProperties(msgId,srcId,properties)
		VertexUpdateProperties(msgId,srcId,properties)
		VertexRemoval(msgId,srcId)
		
		EDGE MESSAGES
		EdgeAdd(msgId,srcId,destID)
		EdgeAddWithProperties(msgId,srcId,dstId,properties)
		EdgeUpdateProperties(msgId,srcId,dstId,properties)
		EdgeRemoval(msgId,srcId,dstID)
		
		REMOTE EDGE MESSAGES
		RemoteEdgeAdd(msgId,srcId,dstId)
		RemoteEdgeAddWithProperties(msgId,srcId,dstId,properties)
		RemoteEdgeUpdateProperties(msgId,srcId,dstId,properties)
		RemoteEdgeRemoval(msgId,srcId,dstId) 
		
receive function takes either PassPartitionList Case Class (used to initialise Graph Manager with list of children) or JSON string.
	
Json String passed to handler funciton which works out what the message type is and passes it to the appropriate handler for that message type.  
The handler then extracts all the information from the JSON, creates a Case class object and sends this to the corret Graph Partition (child) according to the partitioning stratagy (choose child function) which is currently just a hash of the src ID

</br>
</br>
</br>

###Graph Partition
Takes the partition id on creation and saves into ChildID

Vertices Map [srcID,Vertex Obj]  
Edges Map [(srcID,dstID),Edge Obj]
Partition Map [childID,ActorRef] -- list of all other graph partitions 

Receive function takes PassPartitionList message to give the parition the list of other children, or one of the message types defined in Graph manager - it then passes these to the appropriate handler function

####Vertex Block
For all functions in this block, the print function is called (if logging is enabled) 

**Add Vertex**  
If the vertex does not exist in the Vertices Map it is created, otherwise the revive function is called to add the information to the  existing objects state list.

**Add Vertex with Properties**  
This function calls the one above, then for each property adds it to the vertex object via the '+' function.

**Update Vertex Properties**  
This function does basically the same thing as add with props, but is seperate in case in the future we wish to do something diffrent when updating.

**Delete Vertex**
If the Vertex does not exist it is created, and initialised with a remove first (as discussed above), otherwise the kill function is called. Then for each associated edge the edgeRemoval function is called. 

**Add Edge**  
First we check if there edge is 'local' or 'remote', meaning are both the involvd vertices on this partition, or are they split between this one and another. 

In the case of a local edge, first both vertices are checked to see if they exist. If they do not, they are created, else they are informed of the revive; the edge is then added into the set of associated edges. We then check if the edge exists, if it does it is informed of the revive, else it is created. 

####Print Block
The print function take the entity ID and a msg (string) -- the ID is used to create a file where the msg (entity history) is stored. This way instead of confusing logs that make no sense due to interleavings, each entity has a seperate file which can be checked.  
