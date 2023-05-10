# 3DB (Daniel's Distributed Database)
## What is 3DB?

3DB is a distributed key-value database written in Go, built to handle mission-critical workloads at planetary scale.

## Key Features:
see **Definitions** for italicized terms.

+ **Strong consistency**. Strong consistency refers to the property that concurrent read operations on 3DB will return the same data. 3DB achieves this by using Raft, a consensus algorithm that uses a leader-follower architecture which replicates writes from the leader to its followers with log replication. A quorom must be reached for the write to be committed permenantly to 3DB, see more under log replication.
+ **Leader elections**. Leader elections ensure that only 1 *node* can be the leader (in the *quorum*) at any given time in 3DB. When a new *node* joins the network or an existing *node* believes the leader has failed (see heartbeats), it will begin an election after an *election timeout*. In this period, candidates will attempt to gather votes — the candidate with a majority becomes the leader. At the end of an election, the new leader in 3DB will send a gRPC call to its followers, announcing a new term has started.
+ **Log replication via Replicated State Machine**. Log replication refers to the process in which leader relay writes to its followers. If a write operation to 3DB receives a *quorum* of approvals, the leader will commit the write to its own log and will send an RPC call to its followers telling them to commit the write. This log is important to help restore the system quickly when a *node* fails — a follower can simply use the log stored on disk to restore the most recent state. The leader will also send an entire copy of the log to new nodes added to 3DB.
+ **Key-Value Engine**. The Key-Value Engine is built on top of Raft's strong consistency guarantees. It exposes both gRPC and REST APIs to serve application-level read/write requests The engine employs an LRU caching strategy, as disk reads are time-consuming and storing all the records in memory is impractical.
+ **Multi-cloud**. 3DB supports any type of nodes to be added via IP address. This insulates 3DB's users from the risk of significant downtime from a cloud-provider failure. It also allows 3DB's users to build globally distributed applications and for these applications to have low-latency reads anywhere in the world.

## Definitions:
+ **Cluster**: Cluster refers to all the nodes connected to form 3DB's distributed system.
+ **Election timeout**: If a follower does not receive a heartbeat from its leader within a certain duration of time called an election timeout, it will trigger a Leader election. Election timeouts are tuned using cluster-specific 
+ **Heartbeats**: Leaders remain in power in 3DB via heartbeats, which are messages continously telling followers that they are healthy and their instructions should be respected during a given term (see term). If a node does not receive a heartbeat after an election timeout, it will believe the leader has failed and begin a new election
+ **Node**: A node represents a machine in 3DB's cluster. The machine can be bare metal (like your MacBook) or a virtual machine (like an AWS EC2 instance);
+ **Quorom**: A quorom refers to a majority of nodes within the cluster which must all agree for a transaction to be committed. 
+ **Term**: In 3DB, a term is a logical unit of time that allows nodes to agree on a single leader. Terms are used to settle conflicts between the log entries of different nodes — vital to maintaining strong consistency during machine failures. 


## References:
This project closely follows the teachings of MIT's Distributed Systems course [6.824](http://nil.csail.mit.edu/6.824/2022/general.html). The consensus algorithm used in 3DB is detailed in this [paper](http://nil.csail.mit.edu/6.824/2022/papers/raft-extended.pdf).

## Changelog:
+ **May 10, 2023**: Leader elections are implemented. So exciting!