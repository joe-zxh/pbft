# pbft

`pbft` is an implementation of the PBFT protocol [1]. It uses the Gorums [2] RPC framework for sending messages between replicas.

## Running the examples

We have written an example client located in `cmd/pbftclient` and an example server located in `cmd/pbftserver`.
These can be compiled by running `make`.
They read a configuration file named `pbft.toml` from the working directory.
An example configuration that runs on localhost is included in the root of the project.
To generate public and private keys for the servers, run `cmd/hotstuffkeygen/hotstuffkeygen -p 'r*' -n 4 --hosts 127.0.0.1 --tls keys`.
To start four servers, run `scripts/run_servers.sh` with any desired options.
To start the client, run `cmd/pbftclient/pbftclient`.


## References

[1] Castro, Miguel, and Barbara Liskov. "Practical Byzantine fault tolerance." OSDI. Vol. 99. No. 1999. 1999.

[2] Tormod Erevik Lea, Leander Jehl, and Hein Meling. Towards New Abstractions for Implementing Quorum-based Systems. In 37th International Conference on Distributed Computing Systems (ICDCS), Jun 2017.
