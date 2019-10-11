# Details About Shiva's Jepsen Test

This test implemented a simple key value store in Shiva to test the fault tolerance of the system. The operations that were implemented and tested were: 
  1. ```createTempKVTable```: The creation of a temporary key-value table.
  2. ```get```: Getting a specific key stored in the table.
  3. ```batchGet```: A batch get operation which queries for multiple values instead of one.
  4. ```scan```: A scan operation which iteratores over the entire results of the table.
  5. ```delete```: A delete operation which is used to delete key-value pairs.
  6. ```put```: A put operation which is used to insert key-value pairs.
These operations was run under two simulated failure conditions, namely two provided by Jepsen: ```partition-majorities-ring``` and ```partition-random-halves```. The former option does not often occur in real-world applications, while the latter does (hence I will explain in detail). ```partition-random-halves``` divides a cluster of nodes into two halves in a manner that prevents them from communicating with one another. Jepsen does not actually crash the nodes on which the Shiva servers are running, but rather blocks messages from a certain IP address on a node's IP address table (using the ```iptables``` command in ```ssh```). 

Through comprehensive testing, we have found that read operations are unable to guarantee consistency under certain conditions and have been cited as an area which needs improvement. A variety of performance checkers have been implemented in our test, including a latency checker as well as a history checker (that ascertains the operations are correct). 

Databases are not automatically created by Jepsen, but instead, Shiva clients connect to prebuilt databases that were manually setup. This will not work in a CI (continuous integration) environment where several pull requests might want to run these tests simultaneously. A future development we want to take is help automate the construction of DBs.
