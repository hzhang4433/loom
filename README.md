# Loom

Loom is a concurrent execution framework supporting nested contract transactions that ensures deterministic outcomes with high performance for blockchain systems.

# Preparation
### Clone Project
For a quick clone, use the shallow clone flag --depth 1.
```
git clone --depth 1 https://github.com/hzhang4433/loom.git
```
### Compile Environment
| Software    | Version     |
| :---------: | :---------: |
| Ubuntu      | 20.04.6 LTS |
| GCC         | 11.4.0      |
| G++         | 11.4.0      |
| CMake       | 3.22.1      |
| TBB         | 2020.1      |
| Boost       | 1.71.0      |
| Glog        | 0.4.0       |
| Gflags      | 2.2.2       |
| fmt         | 6.1.2       |

### Building Instructions
We use CMake as the building system.

To configure the building plan, we use the following instructions.
```
cmake -S . -B build
```
To build this project, we use the following command.
```
cmake --build build -j
```
This build command will compile and link the main library along with several executables.

The generated executable is called a bench. Its basic usage is as follows:
```
./build/bench [PROTOCOL] [WORKLOAD] [BENCH TIME]
```
For example:
```
./build/bench Loom:48:9973:1:1 TPCC:1:1600:2:1 2s
```

# Evaluation

