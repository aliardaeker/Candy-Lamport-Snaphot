Ali Arda Eker
Distributed Systems / Assignment 2

Step 1) Initiate branches
	python branch.py b1 9091
	python branch.py b2 9092
	python branch.py b3 9093
	python branch.py b4 9094

Note: Works fine up to 4 branches but when I tested it with 5, socket connection
is not established

Step 2) Initiate controller
	python controller.py 4000 branches.txt

Note: controller listens port 9090 for response messages. This is hard coded as
a global and can be modified.

High Level Implementation:
	Branch`s main creates a Branch class instance and 2 threads. 1 for sending transfer messages and the other one for listening any kind of messages and to response them if necessary. Both threads
shares the same branch instance with the locking mechanism.
	Controller contacts each branch and initiates their balance. Then periodically starts a snapshot and prints the responses collected from each branch. If the command line argument is not divided equally by the number of branches, then controller favors the last branch.
	