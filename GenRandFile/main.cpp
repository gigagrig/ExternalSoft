#include <iostream>

using namespace std;

int main(int argc, char* argv[])
{
    cout << "Hello World!" << endl;

    string filename("INPUT2");
    int size = 1024*1024*64;

    FILE* f = fopen(filename.c_str(), "w+");
    if (!f) {
        cerr << "Failed to open file";
    }

    int currentPercent = 1;
    int onePervent = size / 100;

    for (int i = 0; i < size; ++i) {
        if (i > onePervent*currentPercent) {
            cout << currentPercent  << "% done.\n";
            ++currentPercent;
        }
        int32_t r = rand();
        fwrite(&r, sizeof(r), 1, f);
    }

    fclose(f);


    return 0;
}
