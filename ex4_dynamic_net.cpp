#include <Percolation_Sim.h>
#include <tuple.h>

// This example simulates an epidemic on a changing network using an SIR
// percolation simulation.  Since networks that change with time can be
// implemented in many different ways, here is a more detailed description:
//
// During each infectious period, 10% of all nodes "migrate".  A different
// 10% is randomly selected each period.  If a nodes is in the migrating 
// group, all of its edges are broken; this means the node's neighbors also
// end up with at least one broken edge.  These broken edges ("stubs") are
// then randomly reconnected.  The reconnection process will make sure that
// no self-loops or parallel edges get created (it's possible that
// additional edges will get shuffled in order to get rid of these).

int main() {

    // Construct an undirected network
	Network net("name", Network::Undirected);
	string filename = "testthing"; // TODO parse an arg to get this
    net.read_edgelist(filename);
	
	int deltaT = 10; // TODO parse an arg to get this
	string diffFileName = "othertestthing"; // TODO parse an arg to get this
	EdgeStream edgeStream(diffFileName, deltaT);

    // Create variables to handle node migration
    vector<Node*> nodes = net.get_nodes();
    vector<Edge*> broken_edges;
    double fraction_to_migrate = 0;
    int num_to_migrate = fraction_to_migrate * net.size();

    // Set up simulation
    Percolation_Sim sim(&net);
    sim.set_transmissibility(0.25); // Epidemic size will be 37-38%
    sim.rand_infect(10); // The probability of an epidemic will be ~99%

    // Continue the simulation as long as someone is still infected
    while (sim.count_infected() > 0) {
        sim.step_simulation(); // This advances the simulation by one infectious pd
        tuple<vector<tuple<int,int>>,vector<tuple<int,int>>> diffs = edgeStream.step(); // get the "next" step from edge stream; these can be empty
		vector<tuple<int,int>> added = diffs.get<0>;
		vector<tuple<int,int>> removed = diffs.get<1>;
		for (int i = 0; i < removed.size(); i++) {
		  int node0 = removed[i].get<0>;
		  int node1 = removed[i].get<1>;
		  net.get_node(node0).disconnect_from(net.get_node(node1));
		}
		
		for (int i = 0; i < added.size(); i++) {
		  int node0 = added[i].get<0>;
		  int node1 = added[i].get<1>;
		  net.get_node(node0).connect_to(net.get_node(node1));
		}
	    // print out some stuff? intermediate infection results?
			
	}
    // The following line is not necessary, but validate() will check
    // to make sure that the network structure is still valid.  For example,
    // If Node A has an edge leading to Node B, Node B must know it has an
    // inbound edge from Node A.  In undirected graphs, there must be a 
    // complementary edge from Node B to Node A.  (Technically, these are arcs.) 
    net.validate();

    // Print out the final epidemic size
    cout << sim.epidemic_size() << endl;
    return 0;
}
