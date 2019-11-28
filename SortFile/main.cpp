#include <algorithm>
#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <mutex>
#include <thread>
#include <queue>


using namespace std;

const char* intputFile = "input";
const char* outputFile = "output";


/// Possible exception on sorting process
class SortAlgException : exception {
public:
    SortAlgException(const string& what) : exception() {
        m_what = what;
    }
    const char* what() const noexcept override { return m_what.c_str(); }
private:
    string m_what;
};

/// sorted part of input file
struct sortedSegment
{
    string filename; // temp filename
    int level; // number of merges (starts with 0)
};


/// Smart pointer controlling FILE* instance
typedef unique_ptr<FILE,  function<void(FILE*)>> UniqueFileHandler;

UniqueFileHandler makeUniqueHandler(const char* fileName, const char* mode)
{
    return unique_ptr<FILE, function<void(FILE*)>>(fopen(fileName, mode), [](FILE* f)
        {
            fclose(f);
        });
}


namespace  {
mutex inputMutex; // mutex for reading from input file
atomic_bool isFileRead; // flag, that indicates whether input file was read
atomic_int segmentsCount; // count of read and sorted segments from input
exception_ptr segSortEx = nullptr; // exception occured while parallel sorting
exception_ptr segMergeEx = nullptr; // exception occured while parallel merging
atomic_int merged; // flag, that indicates all segments where merged
queue<sortedSegment> sortedSegments; // queue of sorted segments that need to be merged
mutex queeMutex; // mutex on queue of files
}


string genUniqueFilename() {
    char nameBuf[L_tmpnam];
    tmpnam(nameBuf);
    return string(nameBuf);
}


void sortingSegmentWorker(uint32_t* buffer, size_t bufferSize, FILE* input)
{
    try {
        unique_lock<mutex> inp(inputMutex, defer_lock);
        unique_lock<mutex> quee(inputMutex, defer_lock);
        while (!isFileRead.load())
        {
            inp.lock();
            size_t readCount = fread(buffer, sizeof(uint32_t), bufferSize, input);
            inp.unlock();
            if (readCount == 0) {
                isFileRead.store(true);
                return;
            }

            sort(buffer, buffer + readCount);
            string chunkName = genUniqueFilename();
            auto output = makeUniqueHandler(chunkName.c_str(), "wb");
            if (!output) {
                throw SortAlgException("Failed to create temporary file while sorting segments.");
            }
            fwrite(buffer, sizeof(uint32_t), readCount, output.get());
            quee.lock();
            sortedSegments.push({string(chunkName), 0});
            cout << "chunk " << segmentsCount.fetch_add(1) + 1 << " sorted" << endl;
            quee.unlock();
        }
    }
    catch(...) {
        isFileRead.store(true);
        segSortEx = current_exception();
    }

}

void generateSortedSegmentsParallel(uint32_t* sortingBuffer, size_t bufferSize, FILE* input) {

    constexpr int kWorkersCount = 4;
    thread sortWorkers[kWorkersCount];
    isFileRead.store(false);
    segmentsCount.store(0);
    segSortEx = nullptr;
    const int workerBufSize = bufferSize / kWorkersCount;
    for (int i = 0; i < kWorkersCount; ++i)
        sortWorkers[i] = thread(sortingSegmentWorker, sortingBuffer + i*workerBufSize, workerBufSize, input);
    for (int i = 0; i < kWorkersCount; ++i)
        if (sortWorkers[i].joinable())
            sortWorkers[i].join();

    if (segSortEx) {
        rethrow_exception(segSortEx);
    }
}


void generateSortedSegments(uint32_t* sortingBuffer, size_t bufferSize, FILE* input)
{
    int chunk = 0;
    while(!feof(input)) {
        size_t readCount = fread(sortingBuffer, sizeof(uint32_t), bufferSize, input);
        if (readCount == 0) {
            return;
        }
        sort(sortingBuffer, sortingBuffer + readCount);
        cout << "chunk " << ++chunk << " sorted" << endl;
        const char *chunkName =  tmpnam(0);
        auto output = makeUniqueHandler(chunkName, "wb");
        if (!output) {
            throw SortAlgException("Failed to create temporary file while sorting segments."); //
        }
        fwrite(sortingBuffer, sizeof(uint32_t),  readCount, output.get());
        sortedSegments.push({string(chunkName), 0});
    }
}

void mergeSortedFilesPair(const string& file1, const string& file2, const string& resultName)
{
    auto f1 = makeUniqueHandler(file1.c_str(), "rb");
    auto f2 = makeUniqueHandler(file2.c_str(), "rb");
    if (!f1 || !f2)
        throw SortAlgException("Failed to read temporary file while merging");
    auto output = makeUniqueHandler(resultName.c_str(), "wb");

    FILE* outDesc = output.get();
    if (!output) {
        throw SortAlgException("Failed to create temporary file while merging");
    }

    uint32_t vals[2];
    FILE* descriptors[2] {f1.get(), f2.get()};
    fread(vals, sizeof(uint32_t), 1, descriptors[0]);
    fread(vals+1, sizeof(uint32_t), 1, descriptors[1]);

    unsigned nextIdx = 0;
    for(;;) {
        nextIdx = 0;
        if (vals[1] < vals[0])
            nextIdx = 1;
        fwrite(vals + nextIdx, sizeof(uint32_t), 1, outDesc);
        size_t read = fread(vals + nextIdx, sizeof(uint32_t), 1, descriptors[nextIdx]);
        if (read == 0) {
            break;
        }
    }

    // writing the remaining data in another stream
    nextIdx = (nextIdx + 1) % 2;
    fwrite(vals + nextIdx, sizeof(uint32_t), 1, outDesc);
    while (0 < fread(vals + nextIdx, sizeof(uint32_t), 1, descriptors[nextIdx])) {
        fwrite(vals + nextIdx, sizeof(uint32_t), 1, outDesc);
    }
}


class MinStream
{
public:
    struct heapItem {
        uint32_t value;
        FILE* fileIdx;
        bool operator<(heapItem item) const {
            return this->value > item.value;
        }
    };
    MinStream(const vector<string>& filenames, char *buffer, size_t bufferSize) {
        try {
            const int streamsCount = filenames.size();
            m_handlers.resize(streamsCount);
            const size_t streamBufSize = bufferSize / streamsCount;
            for (int i = 0; i < streamsCount; ++i) {
                m_handlers[i] =  makeUniqueHandler(filenames[i].c_str(), "rb");
                if (!m_handlers[i]) {
                    throw SortAlgException("Failed to read temporary file while merging");
                }
                setvbuf(m_handlers[i].get(), buffer + i*streamBufSize, _IOFBF, streamBufSize);
                uint32_t initValue;
                fread(&initValue, sizeof(uint32_t), 1, m_handlers[i].get());
                m_heapStream.push_back({initValue, m_handlers[i].get()});
            }
            make_heap(m_heapStream.begin(), m_heapStream.end());
        }
        catch(...) {
            m_heapStream.~vector();
            m_handlers.~vector();
            throw;
        }
    }

    uint32_t getNextValue() {
        uint32_t next = m_heapStream[0].value;
        FILE* fileIdx = m_heapStream[0].fileIdx;
        pop_heap(m_heapStream.begin(), m_heapStream.end());
        uint32_t newNext;
        if (fread(&newNext, sizeof(uint32_t), 1, fileIdx) == 1) {
            m_heapStream.back() = {newNext, fileIdx};
            push_heap(m_heapStream.begin(), m_heapStream.end());
        } else {
            m_heapStream.pop_back();
        }
        return next;
    }
    int openStreamsCount() const {
        return m_heapStream.size();
    }

private:
    vector<UniqueFileHandler> m_handlers;
    vector<heapItem> m_heapStream;
};


void mergeSortedFiles(const vector<string>& files, const string& resultName, char *buffer, size_t bufferSize)
{
    auto output = makeUniqueHandler(resultName.c_str(), "wb");

    FILE* outDesc = output.get();
    if (!output) {
        throw SortAlgException("Failed to create temporary file for merging");
    }

    const int outBufSize = bufferSize / (files.size() - 1);
    setvbuf(outDesc, buffer, _IOFBF, outBufSize);
    MinStream valuesStream(files, buffer + outBufSize, bufferSize = outBufSize);
    while (valuesStream.openStreamsCount() > 0) {
        uint32_t val = valuesStream.getNextValue();
        fwrite(&val, sizeof(uint32_t), 1, outDesc);
    }
}


void mergeSortedFileWork(int targetMerges)
{
    sortedSegment f1;
    sortedSegment f2;
    try {
        for(int m = merged.load(); m < targetMerges && m >= 0;  m = merged.load()) {

            // fetching next files to merge
            unique_lock<mutex> queue(queeMutex, defer_lock);
            queue.lock();
            if (sortedSegments.size() < 2) {
                continue;
            }
            f1 = sortedSegments.front();
            sortedSegments.pop();
            f2 = sortedSegments.front();
            sortedSegments.pop();
            int currentMerge = merged.fetch_add(1) + 1;
            queue.unlock();

            // merging to resfile
            string resFile;
            int nextLevel = max(f1.level, f2.level) + 1;
            resFile = currentMerge < targetMerges ? string(tmpnam(0)) : string(outputFile);
            cout << "Merging " << f1.filename << " and " << f2.filename
                 << " to " << resFile
                 << " (" << currentMerge << " of " << targetMerges << ')' << endl;

            mergeSortedFilesPair(f1.filename, f2.filename, resFile);

            queue.lock();
            sortedSegments.push(sortedSegment{resFile, nextLevel});
            queue.unlock();

            // removing temp files
            if (0 != remove(f1.filename.c_str())) {
                cout << "FAILED TO REMOVE FILE " << f1.filename << endl;
            }
            if (0 != remove(f2.filename.c_str())) {
                cout << "FAILED TO REMOVE FILE " << f2.filename << endl;
            }
        }
    }
    catch(...)
    {
        merged.store(-1);
        segMergeEx = current_exception();
        // remove temp files if they still exists
        remove(f1.filename.c_str());
        remove(f2.filename.c_str());
        cout << "Thread " << this_thread::get_id() << " throwed exception" << endl;
        rethrow_exception(segMergeEx);
    }
    cout << "Thread " << this_thread::get_id() << " finished" << endl;
}

void mergeSegmentsParallel()
{
    if (sortedSegments.size() <= 1) {
        return;
    }

    int targetMerges = sortedSegments.size() - 1;

    segMergeEx = nullptr;
    constexpr int kPoolSize = 4;
    thread mergePool[kPoolSize];
    merged.store(0);
    for (int i = 0; i < kPoolSize; ++i)
        mergePool[i] = thread(mergeSortedFileWork, targetMerges);
    for (int i = 0; i < kPoolSize; ++i)
        if (mergePool[i].joinable())
            mergePool[i].join();
    if (segMergeEx) {
        rethrow_exception(segMergeEx);
    }
}


void mergeSegmentsByPairs()
{
    int targetMerges = sortedSegments.size() - 1;
    int currentMerge = 0;
    while (sortedSegments.size() > 1) {
        ++currentMerge;
        sortedSegment f1(sortedSegments.front());
        sortedSegments.pop();
        sortedSegment f2(sortedSegments.front());
        sortedSegments.pop();
        string resFile;
        int nextLevel = max(f1.level, f2.level) + 1;
        resFile = sortedSegments.size() > 0 ? string(tmpnam(0)) : string(outputFile);
        cout << "Merging " << f1.filename << " and " << f2.filename
             << " to " << resFile
             << " (" << currentMerge << " of " << targetMerges << ')' << endl;
        mergeSortedFilesPair(f1.filename, f2.filename, resFile);
        sortedSegments.push(sortedSegment{resFile, nextLevel});
        remove(f1.filename.c_str());
        remove(f2.filename.c_str());
    }
}

void mergeSegmentsDirectly(char* buffer, size_t bufSize)
{
    const size_t kMaxOpenFilesCount = 32;

    while (sortedSegments.size() > 1) {
        size_t size = min(kMaxOpenFilesCount, sortedSegments.size());
        std::vector<string> files;
        for (size_t i = 0; i < size; ++i) {
            sortedSegment f1(sortedSegments.front());
            sortedSegments.pop();
            files.push_back(f1.filename);
        }
        string resFile;
        resFile = sortedSegments.size() > 0 ? string(tmpnam(0)) : string(outputFile);
        mergeSortedFiles(files, resFile, buffer, bufSize);
        sortedSegments.push(sortedSegment{resFile, 0});
        for (size_t i = 0; i < files.size(); ++i) {
            remove(files[i].c_str());
        }
    }
}


void clearTempFiles()
{
    while (sortedSegments.size() > 0) {
        cout << "Removing temp file " << sortedSegments.front().filename.c_str() << endl;
        remove(sortedSegments.front().filename.c_str());
        sortedSegments.pop();
    }
}

bool checkIsSorted(const char* filename)
{
    unique_ptr<FILE,  function<void(FILE*)>> input = makeUniqueHandler(filename, "rb");
    if (!input) {
        cout << "Failed to open file " << filename << endl;
        return false;
    }
    FILE* inputRaw = input.get();
    uint32_t current;
    if (0 == fread(&current, sizeof(uint32_t), 1, inputRaw)) {
        return true;
    }
    uint32_t next;
    while(true) {
        if (0 == fread(&next, sizeof(uint32_t), 1, inputRaw)) {
            return true;
        }
        if (next < current)
            return false;
        current = next;
    }
}


unique_ptr<char[]> allocateMaxBuffer(size_t maxSize, size_t minSize, size_t* size)
{
    *size = maxSize;
    while (true)
    {
        try {
            if (*size < minSize)
                return unique_ptr<char[]>();
            unique_ptr<char[]> buf(new char[*size]);
            if (buf)
                return buf;
            *size = 3 * (*size) / 4;
        }
        catch(std::bad_alloc&) {
            *size = 3 * (*size) / 4;
        }
        catch(...) {
            throw;
        }
    }
}


int sortFileIn2Steps()
{
    try {

        chrono::steady_clock::time_point start = chrono::steady_clock::now();

        unique_ptr<FILE,  function<void(FILE*)>> input = makeUniqueHandler(intputFile, "rb");

        if (!input) {
            cerr << "Failed top open INPUT file." << endl;
            return 1;
        }

        size_t bufSize;
        constexpr size_t kMegabyte = 1024u * 1024u;
        unique_ptr<char[]> buf = allocateMaxBuffer(64u*kMegabyte, kMegabyte, &bufSize);
        if (!buf) {
            cout << "Not enough RAM to sort file." << endl;
            return 2;
        }
        cout << bufSize << " bytes allocated for buffer" << endl;

        generateSortedSegmentsParallel(reinterpret_cast<uint32_t*>(buf.get()),
                                       bufSize / sizeof(uint32_t),
                                       input.get());


        input.reset(); // closing input file

        chrono::steady_clock::time_point endSort = chrono::steady_clock::now();

        auto sortDurationMs = chrono::duration_cast<chrono::milliseconds>(endSort - start).count();

        //merging
        start = chrono::steady_clock::now();
        buf.reset(); // release bufer if no need
        mergeSegmentsParallel();

        chrono::steady_clock::time_point endMerge = chrono::steady_clock::now();

        cout << "Sorting took "
                  << sortDurationMs
                  << "ms." << endl;
        cout << "Merging took "
                  << std::chrono::duration_cast<chrono::milliseconds>(endMerge - start).count()
                  << "ms." << endl;
    }
    catch (exception& ex) {
        cerr << ex.what() << endl;
        clearTempFiles();
        return 3;
    }
    catch (...) {
        cerr << "Internal error : unhandled exception" << endl;
        clearTempFiles();
        return 4;
    }
    return 0;
}

int main()
{
    int res = sortFileIn2Steps();

    return res;
}
