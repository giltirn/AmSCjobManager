// Each rank contributes one element to a vector, then checks the global sum.
#include <mpi.h>

#include <algorithm>
#include <fstream>
#include <iostream>
#include <numeric>
#include <stdexcept>
#include <string>
#include <vector>

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);
    int rank = 0;
    int ranks = 0;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    try {
        if (argc != 3) {
            throw std::runtime_error("Usage: mpi_reduction EXPECTED_RANKS RESULT_FILE");
        }
        std::size_t consumed = 0;
        const long long expected = std::stoll(argv[1], &consumed);
        if (consumed != std::string(argv[1]).size() || expected != ranks) {
            throw std::runtime_error("MPI communicator size " + std::to_string(ranks) +
                                     " does not match expected ranks " + argv[1]);
        }

        // Every rank contributes at its own index; all ranks receive the result.
        std::vector<int> contribution(ranks, 0);
        std::vector<int> reduced(ranks, 0);
        contribution[rank] = 1;
        MPI_Allreduce(contribution.data(), reduced.data(), ranks, MPI_INT, MPI_SUM,
                      MPI_COMM_WORLD);
        const int valid = std::all_of(reduced.begin(), reduced.end(),
                                     [](int value) { return value == 1; });
        int all_valid = 0;
        MPI_Allreduce(&valid, &all_valid, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);
        if (!all_valid) {
            throw std::runtime_error("Reduced vector does not contain one at every index");
        }

        // Only rank zero writes. Close and check the stream before finalizing,
        // so a successful job guarantees that the complete result was written.
        if (rank == 0) {
            const long long sum = std::accumulate(reduced.begin(), reduced.end(), 0LL);
            std::ofstream output(argv[2]);
            output << "{\"sum\": " << sum << ", \"ranks\": " << ranks
                   << ", \"valid\": true}\n";
            output.close();
            if (!output) {
                throw std::runtime_error("Could not write result file: " + std::string(argv[2]));
            }
            std::cout << "MPI reduction verified: sum=" << sum << ", ranks=" << ranks
                      << std::endl;
        }
    } catch (const std::exception& error) {
        std::cerr << "Rank " << rank << ": " << error.what() << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 1);
        return 1;
    }

    MPI_Finalize();
    return 0;
}
