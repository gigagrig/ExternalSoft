#include <iostream>

#include <random>

using namespace std;

int main(int argc, char* argv[])
{
    cout << "Hello World!" << endl;

    string filename("INPUT2");
    size_t size = 1024*1024*1024;

    FILE* f = fopen(filename.c_str(), "w+");
    if (!f) {
        cerr << "Failed to open file";
    }

    size_t currentPercent = 1;
    size_t onePercent = size / 100;

    std::random_device rd;
    std::mt19937 mt(rd());
    std::uniform_int_distribution<uint32_t> dist(1, 0xffffffffu);

    for (size_t i = 0; i < size; ++i) {
        if (i > onePercent * currentPercent) {
            cout << currentPercent  << "% done.\n";
            ++currentPercent;
            srand(i);
        }
        int32_t r = dist(mt);
        fwrite(&r, sizeof(r), 1, f);
    }

    fclose(f);

    return 0;
}
