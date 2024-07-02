# bachelor-thesis
RDMA experiment

How to run the experiment:

Open two Terminals one on each node.

Pull this repository onto both of them.

In the Terminal of node02 navigate to ~/bachelor-thesis/node02

Now use "cargo run"

Your Server should be running now and listening for incomming connections from node01 für TCP and RDMA transmissions.

In the Terminal of node01 navigate to ~/bachelor-thesis/node01

Now use "cargo run"

Now you will be asked to choose between TCP and RDMA as transportaton protocol.

If you choose TCP no further actions from you are needed. 

If you choose RDMA you will be asked to choose between SEND, write and atomic.

After you choose one of them the application will use RDMA as underlying transportation protocol with the choosen operation SEND, write or atomic.


