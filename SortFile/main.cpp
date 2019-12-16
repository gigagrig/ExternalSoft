#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <vector>
#include <fstream>
#include <cstring>



using namespace std;

constexpr size_t kKilobyte = 1024u;
constexpr size_t kMegabyte = 1024u * kKilobyte;


// uniqe filename (without path)
string genUniqueFilename() {

    static  atomic_uint_fast32_t chunkNum;
    char buf[25];
    sprintf(buf, ".tmp.chunk.%lu", chunkNum.fetch_add(1));
    return string(buf);
}


unique_ptr<char[]> allocateMaxBuffer(size_t maxSize, size_t minSize, size_t* size)
{
    *size = maxSize;
    while (true)
    {
        try {
            if (*size < minSize)
                return unique_ptr<char[]>();
            return unique_ptr<char[]>(new char[*size]);
        }
        catch(std::bad_alloc&) {
            *size = (*size) / 2;
        }
        catch(...) {
            throw;
        }
    }
}


void clearTempFiles(const vector<string>& tempFiles)
{
    for (size_t i = 0; i < tempFiles.size(); i++) {
        remove(tempFiles[i].c_str());
    }
}


void countAndWrite(ofstream& out, string inputName, uint32_t msb,
                   char* countersBuf, char* ioBuf, size_t ioBufSize)
{
    cout << "Counting for msb " << msb << ", file " << inputName << endl;
    ifstream inp(inputName, ios_base::binary | ios_base::in);
    if (!inp.is_open()) {
        cerr << "Failed to open " << inputName << " file." << endl;
        return;
    }

    constexpr size_t kMax24bitValue = 0x00ffffffu;
    //uint32_t (*counters)[kMax24bitValue + 1];
    //counters = (decltype(counters))countersBuf;
    uint32_t* counters = (uint32_t*)countersBuf;


    uint32_t* inputValues = (uint32_t*)ioBuf;
    bool hasValues = false;
    for(;;) {
        streamsize read = inp.read(ioBuf, ioBufSize).gcount() / sizeof(uint32_t);
        if (read == 0) {
            break;
        }
        hasValues = true;
        for(streamsize i = 0; i < read; i++) {
            uint32_t val = inputValues[i] & 0x00ffffffu;
            counters[val] += 1;
        }
    }

    if (!hasValues)
        return;

    size_t writeBufItemsSize = ioBufSize / sizeof(uint32_t);
    uint32_t *items = (uint32_t*)ioBuf;
    uint32_t buffered = 0;
    uint32_t count;
    uint32_t mask = msb << 24;
    for (uint32_t i = 0; i <= kMax24bitValue; i++) {
        if (counters[i] == 0) {
            continue;
        }
        count = counters[i];
        counters[i] = 0;
        uint32_t number = i | mask;
        while (count > 0) {
            items[buffered] = number;
            buffered++;
            count--;
            if (buffered == writeBufItemsSize) {
                out.write((char*)items, ioBufSize);
                buffered = 0;
            }
        }
    }
    out.write(ioBuf, buffered*sizeof(uint32_t));
}

int sortByCounters(const char* inputFile, const char* outputFile)
{
    chrono::steady_clock::time_point start = chrono::steady_clock::now();

    ifstream inp(inputFile, ios_base::binary | ios_base::in);

    if (!inp.is_open()) {
        cerr << "Failed to open " << inputFile << " file." << endl;
        return 1;
    }

    constexpr size_t kBufSize = 64*kMegabyte;
    size_t tmp;
    unique_ptr<char[]> buf = allocateMaxBuffer(64*kMegabyte, 64*kMegabyte, &tmp);
    if (!buf) {
        cout << "Not enough RAM to sort file." << endl;
        return 2;
    }
    cout << kBufSize << " bytes allocated for buffer" << endl;

    constexpr size_t kIoBufSize =  64 * kKilobyte;
    unique_ptr<char[]> ioBuf = allocateMaxBuffer(kIoBufSize, kIoBufSize,  &tmp);
    if (!ioBuf) {
        cout << "Not enough RAM to sort file." << endl;
        return 2;
    }
    cout << kIoBufSize << " bytes allocated for read buffer" << endl;


    vector<string> tmpNames(256);
    for (size_t i = 0; i < tmpNames.size(); i++) {
        tmpNames[i] = genUniqueFilename();
    }
    ofstream streams[256];
    for (size_t i = 0; i < tmpNames.size(); i++) {
        streams[i].open(tmpNames[i], ios_base::binary | ios_base::out | ios_base::trunc);
        if (!streams[i].is_open()) {
            cerr << "Failed to open " << tmpNames[i] << " file." << endl;
            clearTempFiles(tmpNames);
            return 2;
        }
    }

    constexpr uint32_t kWriteBufItemsSize = kBufSize/(256*sizeof(uint32_t));
    uint32_t (*writeBuffers)[256][kWriteBufItemsSize];
    writeBuffers = (decltype(writeBuffers))buf.get();
    uint32_t addCounters[256];
    memset(addCounters, 0, sizeof(addCounters));


    uint32_t* inputValues = (uint32_t*)ioBuf.get();
    streamsize readSum = 0;
    for(;;) {
        streamsize read = inp.read((char*)inputValues, kIoBufSize).gcount() / sizeof(uint32_t);
        if (read == 0) {
            break;
        }
        readSum += read*sizeof(uint32_t);
        if (readSum % (64*kMegabyte) == 0) {
            cout << readSum / kMegabyte << " MB " << "read" << endl;
        }
        for(streamsize i = 0; i < read; i++) {
            uint8_t msByte = inputValues[i] >> 24;
            (*writeBuffers)[msByte][addCounters[msByte]] = inputValues[i];
            addCounters[msByte] += 1;
            if (addCounters[msByte] == kWriteBufItemsSize) {
                streams[msByte].write((char*)(*writeBuffers)[msByte], sizeof(uint32_t) * addCounters[msByte]);
                addCounters[msByte] = 0;
            }
        }
    }
    for (uint32_t msByte = 0; msByte < 256u; msByte++) {
        streams[msByte].write((char*)(*writeBuffers)[msByte], sizeof(uint32_t) * addCounters[msByte]);
    }
    for (size_t i = 0; i < tmpNames.size(); i++) {
        streams[i].close();
    }

    chrono::steady_clock::time_point endSplit = chrono::steady_clock::now();



    ofstream out(outputFile, ios_base::binary | ios_base::out | ios_base::trunc);
    memset(buf.get(), 0, kBufSize);
    for (size_t i = 0; i < tmpNames.size(); i++) {
        countAndWrite(out, tmpNames[i], i, buf.get(), ioBuf.get(), kIoBufSize);
    }

    out.close();

    clearTempFiles(tmpNames);

    chrono::steady_clock::time_point endSort = chrono::steady_clock::now();
    auto splitDurationMs = chrono::duration_cast<chrono::milliseconds>(endSplit - start).count();
    auto sortDurationMs = chrono::duration_cast<chrono::milliseconds>(endSort - start).count();

    cout << "Splitting by msb took "
              << splitDurationMs
              << "ms." << endl;

    cout << "Sorting took "
              << sortDurationMs
              << "ms." << endl;


    return 0;
}


int main()
{
    int res = sortByCounters("input", "output");
    return res;
}
