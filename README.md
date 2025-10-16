# Enjima
A Resource-Adaptive Stream Processing System written in C++
## Building Enjima

### Installing pre-requisites

The build system uses CMake. In order to build this repository, you need C++20 or later and CMake 3.22 or later. It has currently being tested with only GCC (>= 13) on Ubuntu. Install the required dependencies using the following command

```
sudo apt install build-essential gcc-13 g++-13
sudo apt install cmake
```

If there are multiple gcc versions installed, make sure gcc and g++ are configured to point to g++-13 or later. E.g. if both gcc/g++ 12 and 13 are installed, you can use update-alternatives as follows:

```
sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-12 12
sudo update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-12 12
sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-13 13
sudo update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-13 13
```

### Clone the repository

In order to clone the repository, use

```
git clone git@github.com:lasanthafdo/enjima-lib.git
cd enjima-lib
```

### Build using CMake

Follow the steps below to build Enjima in release mode:

```
mkdir cmake-release-build && cd cmake-release-build
cmake -DCMAKE_BUILD_TYPE=Release ..
cmake --build . -j
```

## Running Enjima

The benchmark programs for Enjima are provided in the [enjima-bench](https://github.com/lasanthafdo/enjima-bench) repository (https://github.com/lasanthafdo/enjima-bench). Please follow the instructions in that repository to run the benchmarks after building the Enjima repository.
