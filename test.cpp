#include <iostream>
#include <vector>
#include <list>
#include <stack>
#include <algorithm>
#include <chrono>

using namespace std;

class Graph {
    int V; // Number of vertices
    list<int> *adj; // Adjacency list representation

    // Helper function for DFS
    void dfs(int u, vector<bool> &blocked, vector<list<int>> &B, stack<int> &st, vector<vector<int>> &cycles, int start);

public:
    Graph(int V);
    void addEdge(int v, int w);
    vector<vector<int>> johnson();
};

Graph::Graph(int V) {
    this->V = V;
    adj = new list<int>[V];
}

void Graph::addEdge(int v, int w) {
    adj[v].push_back(w);
}

void Graph::dfs(int u, vector<bool> &blocked, vector<list<int>> &B, stack<int> &st, vector<vector<int>> &cycles, int start) {
    blocked[u] = true;
    st.push(u);

    for (int v : adj[u]) {
        if (v == start) {
            stack<int> st_copy = st;
            vector<int> cycle;
            while (!st_copy.empty()) {
                cycle.push_back(st_copy.top());
                st_copy.pop();
            }
            // sort(cycle.begin(), cycle.end());
            // if (find(cycles.begin(), cycles.end(), cycle) == cycles.end()) {
            //     cycles.push_back(cycle);
            // }
            cycles.push_back(cycle);
        } else if (!blocked[v]) {
            dfs(v, blocked, B, st, cycles, start);
        }
    }

    st.pop();
    blocked[u] = false;
    for (int w : B[u]) {
        if (!blocked[w]) {
            dfs(w, blocked, B, st, cycles, start);
        }
    }
    B[u].clear();
}

vector<vector<int>> Graph::johnson() {
    vector<vector<int>> cycles;
    vector<bool> blocked(V, false);
    vector<list<int>> B(V);
    stack<int> st;

    for (int s = 0; s < V; ++s) {
        fill(blocked.begin(), blocked.end(), false);
        for (int i = 0; i < V; ++i) {
            B[i].clear();
        }
        dfs(s, blocked, B, st, cycles, s);
    }

    return cycles;
}

// int main() {
//     // Create graph with 10 vertices
//     Graph g(10);

//     // Adding edges as per the given structure
//     for (int i = 0; i < 10; ++i) {
//         for (int j = 0; j < 10; ++j) {
//             if (i != j) {
//                 g.addEdge(i, j);
//             }
//         }
//     }

//     auto start = chrono::high_resolution_clock::now();
//     vector<vector<int>> cycles = g.johnson();
//     auto end = chrono::high_resolution_clock::now();
//     cout << "Time taken: " << chrono::duration_cast<chrono::milliseconds>(end - start).count() << "ms" << endl;

//     cout << "Cycles in the graph:" << endl;
//     cout << "Size: " << cycles.size() << endl;
//     // for (const auto &cycle : cycles) {
//     //     for (int vertex : cycle) {
//     //         cout << vertex + 1 << " ";
//     //     }
//     //     cout << endl;
//     // }

//     return 0;
// }
