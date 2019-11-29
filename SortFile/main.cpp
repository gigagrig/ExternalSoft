#include <algorithm>
#include <atomic>
#include <chrono>
#include <list>
#include <iostream>
#include <memory>
#include <mutex>
#include <vector>
#include <thread>


using namespace std;

constexpr size_t kKilobyte = 1024u;
constexpr size_t kMegabyte = 1024u * kKilobyte;



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
int segmentsCount; // count of read and sorted segments from input
exception_ptr segSortEx = nullptr; // exception occured while parallel sorting
exception_ptr segMergeEx = nullptr; // exception occured while parallel merging
atomic_int merged; // flag, that indicates all segments where merged
list<string> sortedSegments; // queue of sorted segments that need to be merged
mutex queeMutex; // mutex on queue of files
}

// uniqe filename (without path)
// TODO: replace with more proper crossplatform function
string genUniqueFilename() {
    char nameBuf[L_tmpnam];
    tmpnam(nameBuf);
    string name(nameBuf);
    // replacing /\ on .
    size_t p = name.find_first_of("/""\"""");
    for( ; p != string::npos; p = name.find_first_of("/""\"""")) {
        name.replace(p, 1, ".");
    }
    return name;
}

// worker that sorts segments of file INPUT
// sorts until file has been read or maxsegments count reached
void sortingSegmentWorker(uint32_t* buffer, size_t bufferSize, FILE* input, int maxSegmentsCount)
{
    try {
        unique_lock<mutex> inputLocker(inputMutex, defer_lock);
        unique_lock<mutex> queeLocker(inputMutex, defer_lock);
        while (!isFileRead.load())
        {
            inputLocker.lock();
            if (segmentsCount >= maxSegmentsCount)
                break;
            size_t readCount = fread(buffer, sizeof(uint32_t), bufferSize, input);
            if (readCount == 0) {
                isFileRead.store(true);
                return;
            }
            int current_segment = ++segmentsCount;
            inputLocker.unlock();

            sort(buffer, buffer + readCount);
            string chunkName = genUniqueFilename();
            auto output = makeUniqueHandler(chunkName.c_str(), "wb");
            if (!output) {
                throw SortAlgException("Failed to create temporary file while sorting segments.");
            }
            fwrite(buffer, sizeof(uint32_t), readCount, output.get());
            queeLocker.lock();
            sortedSegments.push_back(string(chunkName));
            cout << "chunk " << current_segment << " sorted" << endl;
            queeLocker.unlock();
        }
    }
    catch(...) {
        isFileRead.store(true);
        segSortEx = current_exception();
    }
}

// sorts file parts to temp files
// until file has been read or maxsegments count reached
// TODO : (names saves to list sortedSegments) -> hide global var from API
void generateSortedSegmentsParallel(uint32_t* sortingBuffer, size_t bufferSize, FILE* input,
                                    int numOfThreads,
                                    int maxSegmentsCount = 64 * 1024) {

    const int workersCount = numOfThreads;
    thread sortWorkers[workersCount];
    isFileRead.store(false);
    segmentsCount = 0;
    segSortEx = nullptr;
    const int workerBufSize = bufferSize / workersCount;
    for (int i = 0; i < workersCount; ++i)
        sortWorkers[i] = thread(sortingSegmentWorker,
                                sortingBuffer + i*workerBufSize,
                                workerBufSize,
                                input,
                                maxSegmentsCount);
    for (int i = 0; i < workersCount; ++i)
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
        string chunkName =  genUniqueFilename();
        auto output = makeUniqueHandler(chunkName.c_str(), "wb");
        if (!output) {
            throw SortAlgException("Failed to create temporary file while sorting segments."); //
        }
        fwrite(sortingBuffer, sizeof(uint32_t),  readCount, output.get());
        sortedSegments.push_back(chunkName);
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


void mergeSortedFileWorker(int targetMerges)
{
    string f1;
    string f2;
    try {
        for(int m = merged.load(); m < targetMerges && m >= 0;  m = merged.load()) {
            // fetching next files to merge
            unique_lock<mutex> queueLocker(queeMutex, defer_lock);
            queueLocker.lock();
            if (sortedSegments.size() < 2) {
                continue;
            }
            f1 = sortedSegments.front();
            sortedSegments.pop_front();
            f2 = sortedSegments.front();
            sortedSegments.pop_front();
            int currentMerge = merged.fetch_add(1) + 1;
            queueLocker.unlock();

            // merging to resfile
            string resFile = genUniqueFilename();
            cout << "Merging " << f1 << " and " << f2
                 << " to " << resFile
                 << " (" << currentMerge << " of " << targetMerges << ')' << endl;

            mergeSortedFilesPair(f1, f2, resFile);

            queueLocker.lock();
            sortedSegments.push_back(resFile);
            queueLocker.unlock();

            // removing temp files
            if (0 != remove(f1.c_str())) {
                cout << "FAILED TO REMOVE FILE " << f1 << endl;
            }
            if (0 != remove(f2.c_str())) {
                cout << "FAILED TO REMOVE FILE " << f2 << endl;
            }
        }
    }
    catch(...)
    {
        merged.store(-1);
        segMergeEx = current_exception();
        // remove temp files if they still exists
        remove(f1.c_str());
        remove(f2.c_str());
        cout << "Thread " << this_thread::get_id() << " throwed exception" << endl;
        rethrow_exception(segMergeEx);
    }
}

// merges sortedSegments
// if previousSegmentsFile is specified, it merges in the end.
void mergeSegmentsParallel(int numOfThreads)
{
    if (sortedSegments.size() <= 1) {
        return;
    }

    int targetMerges = sortedSegments.size() - 1;

    segMergeEx = nullptr;
    merged.store(0);

    thread mergePool[numOfThreads];
    for (int i = 0; i < numOfThreads; ++i)
        mergePool[i] = thread(mergeSortedFileWorker, targetMerges);
    for (int i = 0; i < numOfThreads; ++i)
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
        string f1(sortedSegments.front());
        sortedSegments.pop_front();
        string f2(sortedSegments.front());
        sortedSegments.pop_front();
        string resFile;
        resFile = genUniqueFilename();
        cout << "Merging " << f1 << " and " << f2
             << " to " << resFile
             << " (" << currentMerge << " of " << targetMerges << ')' << endl;
        mergeSortedFilesPair(f1, f2, resFile);
        sortedSegments.push_back(resFile);
        remove(f1.c_str());
        remove(f2.c_str());
    }
}

void mergeSegmentsDirectly(char* buffer, size_t bufSize)
{
    const size_t kMaxOpenFilesCount = 32;

    while (sortedSegments.size() > 1) {
        size_t size = min(kMaxOpenFilesCount, sortedSegments.size());
        std::vector<string> files;
        for (size_t i = 0; i < size; ++i) {
            string f1(sortedSegments.front());
            sortedSegments.pop_front();
            files.push_back(f1);
        }
        string resFile;
        resFile = genUniqueFilename();
        mergeSortedFiles(files, resFile, buffer, bufSize);
        sortedSegments.push_back(resFile);
        for (size_t i = 0; i < files.size(); ++i) {
            remove(files[i].c_str());
        }
    }
}


void clearTempFiles(list<string> tempFiles)
{
    while (tempFiles.size() > 0) {
        cout << "Removing temp file " << sortedSegments.front() << endl;
        remove(tempFiles.front().c_str());
        tempFiles.pop_front();
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

// sort input file to output
// TODO : replace gloabal params(names of files) to function parameters
// spliting whole file to sorted segments
// uniting segments after
int sortFileIn2Steps(const char* inputFile, const char* outputFile, size_t maxBufferSize, int numOfThreads)
{
    try {

        chrono::steady_clock::time_point start = chrono::steady_clock::now();

        unique_ptr<FILE,  function<void(FILE*)>> input = makeUniqueHandler(inputFile, "rb");

        if (!input) {
            cerr << "Failed to open INPUT file." << endl;
            return 1;
        }

        size_t bufSize;
        unique_ptr<char[]> buf = allocateMaxBuffer(maxBufferSize, kKilobyte,  &bufSize);
        if (!buf) {
            cout << "Not enough RAM to sort file." << endl;
            return 2;
        }
        cout << bufSize << " bytes allocated for buffer" << endl;

        generateSortedSegmentsParallel(reinterpret_cast<uint32_t*>(buf.get()),
                                       bufSize / sizeof(uint32_t),
                                       input.get(),
                                       numOfThreads);


        input.reset(); // closing input file

        chrono::steady_clock::time_point endSort = chrono::steady_clock::now();

        auto sortDurationMs = chrono::duration_cast<chrono::milliseconds>(endSort - start).count();

        //merging
        start = chrono::steady_clock::now();
        buf.reset(); // release bufer if no need

        mergeSegmentsParallel(numOfThreads);

        if (sortedSegments.empty())
            return 0;
        if (0 != rename(sortedSegments.front().c_str(), outputFile)) {
            cout << "Failed to create output file " << outputFile << endl;
        }
        sortedSegments.pop_back();

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
        clearTempFiles(sortedSegments);
        return 3;
    }
    catch (...) {
        cerr << "Internal error : unhandled exception" << endl;
        clearTempFiles(sortedSegments);
        return 4;
    }
    return 0;
}

// sort input file to output
// TODO : replace gloabal params(names of files) to function parameters
// 1) spliting @param sizeOfQueue segments
// 2) uniting segments to nextLevelQueue
// repeats 1-2 until input's been read
int sortFileStepByStep(const char* inputFile, const char* outputFile,
                       size_t maxBufferSize, int numOfThreads, int sizeOfQueue)
{
    list<string> mergedSegmentsQuee;
    try {
        chrono::steady_clock::time_point start = chrono::steady_clock::now();

        unique_ptr<FILE,  function<void(FILE*)>> input = makeUniqueHandler(inputFile, "rb");

        if (!input) {
            cerr << "Failed to open INPUT file." << endl;
            return 1;
        }

        size_t bufSize;
        unique_ptr<char[]> buf = allocateMaxBuffer(maxBufferSize, kKilobyte, &bufSize);
        if (!buf) {
            cout << "Not enough RAM to sort file." << endl;
            return 2;
        }
        cout << bufSize << " bytes allocated for buffer" << endl;

        while (!feof(input.get())) {
            generateSortedSegmentsParallel(reinterpret_cast<uint32_t*>(buf.get()),
                                           bufSize / sizeof(uint32_t),
                                           input.get(),
                                           numOfThreads,
                                           sizeOfQueue);
            // merging
            mergeSegmentsParallel(numOfThreads);
            if (!sortedSegments.empty()) {
                mergedSegmentsQuee.push_back(sortedSegments.front());
                sortedSegments.pop_front();
            }
        }

        input.reset(); // closing input file
        buf.reset(); // release bufer if no need

        if (!mergedSegmentsQuee.empty()) {
            sortedSegments = mergedSegmentsQuee;
            cout << "Input's been read. Sorting 2nd level tree" << endl;
            mergeSegmentsParallel(numOfThreads);
        }

        chrono::steady_clock::time_point endSort = chrono::steady_clock::now();

        if (sortedSegments.empty())
            return 0;
        if (0 != rename(sortedSegments.front().c_str(), outputFile)) {
            cout << "Failed to create output file " << outputFile << endl;
        }

        auto sortDurationMs = chrono::duration_cast<chrono::milliseconds>(endSort - start).count();

        cout << "Sorting took "
                  << sortDurationMs
                  << "ms." << endl;
    }
    catch (exception& ex) {
        cerr << ex.what() << endl;
        clearTempFiles(sortedSegments);
        clearTempFiles(mergedSegmentsQuee);
        return 3;
    }
    catch (...) {
        cerr << "Internal error : unhandled exception" << endl;
        clearTempFiles(sortedSegments);
        clearTempFiles(mergedSegmentsQuee);
        return 4;
    }
    return 0;
}


int main()
{
    int res = sortFileStepByStep("input", "output", 4*kMegabyte,  4, 256);

    return res;
}
