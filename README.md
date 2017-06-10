## Execute
Run the Terminal and type help for help messages on how to create the Nodes and make queries

## About
A simple implementation of the chord protocol [1]. <br />


### Functionality
  - join, arrival of nodes<br />
  - insert,delete,query<br />
  
We extended the simple implementation with 2 different replication protocols:

### Chain replication(linearization)
In this case there is a main-coordinator node for each interval of keys.<br />
Writes are first transmitted to the coordinator Nodes, who propagate it to all the other replicas <br />
Last Node returns the successful update back to the user. <br />
Reads are transmitted to the last Node with replicas. <br />

### Eventual consistency
Here writes are propagated to the coordinator Node, but succesfull write returns <br />
immediately after to the user. After that the coordinator node has to inform all replicas <br />
Reads can happen at any Node with replicas, with the danger of stale value. <br />






[1] Stoica, Ion, et al. "Chord: A scalable peer-to-peer lookup service for internet applications."
ACM SIGCOMM Computer Communication Review 31.4 (2001): 149-160. 
