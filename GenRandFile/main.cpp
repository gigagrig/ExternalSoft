#include <iostream>

using namespace std;

int main(int argc, char* argv[])
{
    cout << "Hello World!" << endl;

    string filename("INPUT2");
    size_t size = 256*1024*1024;

    FILE* f = fopen(filename.c_str(), "w+");
    if (!f) {
        cerr << "Failed to open file";
    }

    size_t currentPercent = 1;
    size_t onePervent = size / 100;

    for (size_t i = 0; i < size; ++i) {
        if (i > onePervent*currentPercent) {
            cout << currentPercent  << "% done.\n";
            ++currentPercent;
        }
        int32_t r = rand() * rand();
        fwrite(&r, sizeof(r), 1, f);
    }

    fclose(f);


    return 0;
}
