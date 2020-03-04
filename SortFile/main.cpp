#include <algorithm>
#include <atomic>
#include <chrono>
#include <list>
#include <iostream>
#include <memory>
#include <mutex>
#include <vector>
#include <thread>
#include <fstream>
#include <cstring>



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
mutex s_inputMutex; // mutex for reading from input file
atomic_bool s_isFileRead; // flag, that indicates whether input file was read
int s_segmentsCount; // count of read and sorted segments from input
exception_ptr s_segSortEx = nullptr; // exception occured while parallel sorting
exception_ptr s_segMergeEx = nullptr; // exception occured while parallel merging
atomic_int s_merged; // flag, that indicates all segments where merged
list<string> s_sortedSegments; // queue of sorted segments that need to be merged
mutex s_queeMutex; // mutex on queue of files
}

// uniqe filename (without path)
string genUniqueFilename() {

    static  atomic_uint_fast32_t chunkNum;
    char buf[25];
    sprintf(buf, ".tmp.chunk.%lu", chunkNum.fetch_add(1));
    return string(buf);
}

// worker that sorts segments of file INPUT
// sorts until file has been read or maxsegments count reached
void sortingSegmentWorker(uint32_t* buffer, size_t bufferSize, FILE* input, int maxSegmentsCount)
{
    try {
        unique_lock<mutex> inputLocker(s_inputMutex, defer_lock);
        unique_lock<mutex> queeLocker(s_inputMutex, defer_lock);
        while (!s_isFileRead.load())
        {
            inputLocker.lock();
            if (s_segmentsCount >= maxSegmentsCount)
                break;
            size_t readCount = fread(buffer, sizeof(uint32_t), bufferSize, input);
            if (readCount == 0) {
                s_isFileRead.store(true);
                return;
            }
            int current_segment = ++s_segmentsCount;
            inputLocker.unlock();

            sort(buffer, buffer + readCount);
            string chunkName = genUniqueFilename();
            auto output = makeUniqueHandler(chunkName.c_str(), "wb");
            if (!output) {
                throw SortAlgException("Failed to create temporary file while sorting segments.");
            }
            fwrite(buffer, sizeof(uint32_t), readCount, output.get());
            queeLocker.lock();
            s_sortedSegments.push_back(string(chunkName));
            cout << "chunk " << current_segment << " sorted" << endl;
            queeLocker.unlock();
        }
    }
    catch(...) {
        s_isFileRead.store(true);
        s_segSortEx = current_exception();
    }
}

// sorts file parts to temp files
// until file has been read or maxsegments count reached
// TODO : (names saves to list sortedSegments) -> hide global var from API
list<string> generateSortedSegmentsParallel(uint32_t* sortingBuffer, size_t bufferSize, FILE* input,
                                    int numOfThreads,
                                    int maxSegmentsCount = 64 * 1024) {

    const int workersCount = numOfThreads;
    thread sortWorkers[workersCount];
    s_isFileRead.store(false);
    s_segmentsCount = 0;
    s_segSortEx = nullptr;
    s_sortedSegments.clear();
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

    if (s_segSortEx) {
        rethrow_exception(s_segSortEx);
    }
    return s_sortedSegments;
}


list<string> generateSortedSegmentsWithoutMerge(uint32_t* sortingBuffer, size_t bufferSize,
                                    FILE* input, size_t maxSegmentsCount = 64 * 1024)
{
    int chunk = 0;
    list<string> segments;
    while(!feof(input) && segments.size() < maxSegmentsCount) {
        size_t readCount = fread(sortingBuffer, sizeof(uint32_t), bufferSize, input);
        if (readCount == 0) {
            return segments;
        }
        sort(sortingBuffer, sortingBuffer + readCount);
        cout << "chunk " << ++chunk << " sorted" << endl;
        string chunkName =  genUniqueFilename();
        auto output = makeUniqueHandler(chunkName.c_str(), "wb");
        if (!output) {
            throw SortAlgException("Failed to create temporary file while sorting segments."); //
        }
        fwrite(sortingBuffer, sizeof(uint32_t),  readCount, output.get());
        segments.push_back(chunkName);
    }
    return segments;
}

void sortParralel(uint32_t* sortingBuffer, size_t size)
{
    thread s1(sort<uint32_t*>, sortingBuffer, sortingBuffer + size / 4);
    thread s2(sort<uint32_t*>, sortingBuffer + size / 4, sortingBuffer + 2*size / 4);
    thread s3(sort<uint32_t*>, sortingBuffer + 2*size / 4, sortingBuffer + 3*size / 4);
    sort<uint32_t*>(sortingBuffer + 3*size / 4, sortingBuffer + size);
    s1.join();
    s2.join();
    s3.join();
    //s1 = thread(inplace_merge<uint32_t*>, sortingBuffer, sortingBuffer + size / 4, sortingBuffer + 2*size / 4);
    inplace_merge<uint32_t*>(sortingBuffer, sortingBuffer + size / 4, sortingBuffer + 2*size / 4);
    inplace_merge(sortingBuffer + 2*size / 4, sortingBuffer + 3*size / 4, sortingBuffer + size);
    //s1.join();
    inplace_merge(sortingBuffer, sortingBuffer + size / 2, sortingBuffer + size);
}

list<string> generateSortedSegments(uint32_t* sortingBuffer, size_t bufferSize,
                                    FILE* input, size_t maxSegmentsCount = 64 * 1024)
{
    int chunk = 0;
    list<string> segments;
    while(segments.size() < maxSegmentsCount) {
        //input.read((char*)sortingBuffer, bufferSize * sizeof(uint32_t));
        //size_t readCount = input.gcount() / sizeof(uint32_t);
        size_t readCount = fread((char*)sortingBuffer, sizeof(uint32_t), bufferSize, input);
        if (readCount == 0) {
            return segments;
        }
        //sort(sortingBuffer, sortingBuffer + readCount);
        sortParralel(sortingBuffer, readCount);

        cout << "chunk " << ++chunk << " sorted" << endl;
        string chunkName =  genUniqueFilename();
        ofstream output(chunkName.c_str(), ios_base::out | ios_base::binary | ios_base::trunc);
        if (!output.is_open()) {
            throw SortAlgException("Failed to create temporary file while sorting segments."); //
        }
        output.write((char*)sortingBuffer, sizeof(uint32_t)*readCount);
        segments.push_back(chunkName);
    }
    return segments;
}

list<string> genSortedSegments(uint32_t* sortingBuffer, size_t bufferSize, FILE* input,
                                    int numOfThreads,
                                    int maxSegmentsCount = 64 * 1024)
{
    if (numOfThreads > 1) {
        return generateSortedSegmentsParallel(sortingBuffer, bufferSize, input,
                                              numOfThreads, maxSegmentsCount);
    }
    // else
    return generateSortedSegments(sortingBuffer, bufferSize, input, maxSegmentsCount);
}

void mergeSortedFilesPair(const string& file1, const string& file2, const string& resultName,
                          char *buf, size_t bufBytes)
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
    FILE* descriptors[2] {f1.get(), f2.get()};

    char* rBuf0 = buf;
    size_t bufTypedsize = (bufBytes / 4) / sizeof(uint32_t);
    char* rBuf1 = buf + bufTypedsize * sizeof(uint32_t);
    uint32_t* outbuf = (uint32_t*)(buf + 2 * bufTypedsize * sizeof(uint32_t));
    size_t merged = 0;
    uint32_t* it0 = 0;
    uint32_t* it1 = 0;
    size_t read0 = fread(rBuf0, sizeof(uint32_t), bufTypedsize, descriptors[0]);
    size_t read1 = fread(rBuf1, sizeof(uint32_t), bufTypedsize, descriptors[1]);
    while (read0 > 0 && read1 > 0) {
        if (it0 == 0)
            it0 = (uint32_t*)rBuf0;
        if (it1 == 0)
            it1 = (uint32_t*)rBuf1;
        while (read0 > 0 && read1 > 0) {
            if (*it0 <= *it1) {
                outbuf[merged] = *it0;
                read0--;
                it0++;
                merged++;
            } else {
                outbuf[merged] = *it1;
                read1--;
                it1++;
                merged++;
            }
        }
        fwrite(outbuf, sizeof(uint32_t), merged, outDesc);
        merged = 0;

        if (read0 == 0) {
            read0 = fread(rBuf0, sizeof(uint32_t), bufTypedsize, descriptors[0]);
            it0 = 0;
        }
        if (read1 == 0) {
            read1 = fread(rBuf1, sizeof(uint32_t), bufTypedsize, descriptors[1]);
            it1 = 0;
        }
    }

    if (read0 > 0 || read1 > 0) {
        uint32_t* it = read0 > 0 ? it0 : it1;
        size_t count = read0 > 0 ? read0 : read1;
        fwrite(it, sizeof(uint32_t), count, outDesc);
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
    MinStream valuesStream(files, buffer + outBufSize, bufferSize - outBufSize);
    while (valuesStream.openStreamsCount() > 0) {
        uint32_t val = valuesStream.getNextValue();
        fwrite(&val, sizeof(uint32_t), 1, outDesc);
    }
}


void mergeSortedFileWorker(int targetMerges, char* mergeBuf, size_t bufsize)
{
    string f1;
    string f2;
    try {
        for(int m = s_merged.load(); m < targetMerges && m >= 0;  m = s_merged.load()) {
            // fetching next files to merge
            unique_lock<mutex> queueLocker(s_queeMutex, defer_lock);
            queueLocker.lock();
            if (s_sortedSegments.size() < 2) {
                continue;
            }
            f1 = s_sortedSegments.front();
            s_sortedSegments.pop_front();
            f2 = s_sortedSegments.front();
            s_sortedSegments.pop_front();
            int currentMerge = s_merged.fetch_add(1) + 1;
            queueLocker.unlock();

            // merging to resfile
            string resFile = genUniqueFilename();
            cout << "Merging " << f1 << " and " << f2
                 << " to " << resFile
                 << " (" << currentMerge << " of " << targetMerges << ')' << endl;

            mergeSortedFilesPair(f1, f2, resFile, mergeBuf, bufsize);

            queueLocker.lock();
            s_sortedSegments.push_back(resFile);
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
        s_merged.store(-1);
        s_segMergeEx = current_exception();
        // remove temp files if they still exists
        remove(f1.c_str());
        remove(f2.c_str());
        cout << "Thread " << this_thread::get_id() << " throwed exception" << endl;
        rethrow_exception(s_segMergeEx);
    }
}

// merges sortedSegments
// if previousSegmentsFile is specified, it merges in the end.
void mergeSegmentsParallel(list<string>& segments, char* mergebuf, size_t bufsize, int numOfThreads)
{
    if (segments.size() <= 1) {
        return;
    }

    s_sortedSegments = segments;
    int targetMerges = s_sortedSegments.size() - 1;

    s_segMergeEx = nullptr;
    s_merged.store(0);
    thread mergePool[numOfThreads];
    size_t workerBufSize = bufsize / numOfThreads;
    for (int i = 0; i < numOfThreads; ++i) {
        char* workerBuf = mergebuf + i*workerBufSize;
        mergePool[i] = thread(mergeSortedFileWorker, targetMerges, workerBuf, workerBufSize);
    }
    for (int i = 0; i < numOfThreads; ++i)
        if (mergePool[i].joinable())
            mergePool[i].join();
    if (s_segMergeEx) {
        rethrow_exception(s_segMergeEx);
    }
    segments = s_sortedSegments;
}


void mergeSegmentsByPairs(list<string>& segments, char* mergebuf, size_t bufsize)
{
    int targetMerges = segments.size() - 1;
    int currentMerge = 0;
    while (segments.size() > 1) {
        ++currentMerge;
        string f1(segments.front());
        segments.pop_front();
        string f2(segments.front());
        segments.pop_front();
        string resFile;
        resFile = genUniqueFilename();
        cout << "Merging " << f1 << " and " << f2
             << " to " << resFile
             << " (" << currentMerge << " of " << targetMerges << ')' << endl;
        mergeSortedFilesPair(f1, f2, resFile, mergebuf, bufsize);
        segments.push_back(resFile);
        remove(f1.c_str());
        remove(f2.c_str());
    }
}

void mergeSegmentsDirectly(char* buffer, size_t bufSize)
{
    if (s_sortedSegments.size() <= 1)
        return;

    const size_t kMaxOpenFilesCount = 32;

    list<string> mergedQuee;

    for (int level = 0 ; s_sortedSegments.size() > 1; level++)
    {
        while (s_sortedSegments.size() > 0) {
            size_t size = min(kMaxOpenFilesCount, s_sortedSegments.size());
            std::vector<string> files;
            for (size_t i = 0; i < size; ++i) {
                string f1(s_sortedSegments.front());
                s_sortedSegments.pop_front();
                files.push_back(f1);
            }
            string resFile;
            resFile = genUniqueFilename();
            cout << "merging " << files.size() << " files of level " << level << endl;
            mergeSortedFiles(files, resFile, buffer, bufSize);
            for (size_t i = 0; i < files.size(); ++i) {
                remove(files[i].c_str());
            }
            mergedQuee.push_back(resFile);
        }
        s_sortedSegments = mergedQuee;
        mergedQuee.clear();
    }
}

void mergeSegments(list<string>& segments, char* mergebuf, size_t bufsize, int numOfThreads)
{
    if (numOfThreads > 1) {
        mergeSegmentsParallel(segments, mergebuf, bufsize, numOfThreads);
    } else {
        mergeSegmentsByPairs(segments, mergebuf, bufsize);
    }
}



void clearTempFiles(list<string> tempFiles)
{
    while (tempFiles.size() > 0) {
        cout << "Removing temp file " << s_sortedSegments.front() << endl;
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

        list<string> segments = genSortedSegments(reinterpret_cast<uint32_t*>(buf.get()),
                                       bufSize / sizeof(uint32_t),
                                       input.get(),
                                       numOfThreads);


        input.reset(); // closing input file

        chrono::steady_clock::time_point endSort = chrono::steady_clock::now();

        auto sortDurationMs = chrono::duration_cast<chrono::milliseconds>(endSort - start).count();

        //merging
        start = chrono::steady_clock::now();

        mergeSegments(segments, buf.get(),  bufSize, numOfThreads);

        buf.reset(); // release bufer if no need

        if (segments.empty())
            return 0;
        if (0 != rename(segments.front().c_str(), outputFile)) {
            cout << "Failed to create output file " << outputFile << endl;
        }
        segments.pop_back();

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
        clearTempFiles(s_sortedSegments);
        return 3;
    }
    catch (...) {
        cerr << "Internal error : unhandled exception" << endl;
        clearTempFiles(s_sortedSegments);
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
                       size_t maxBufferSize, int numOfThreads, size_t sizeOfQueue)
{
    std::vector<list<string>> mergedSegmentsQuee(1);
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
            mergedSegmentsQuee[0] = genSortedSegments(reinterpret_cast<uint32_t*>(buf.get()),
                                           bufSize / sizeof(uint32_t),
                                           input.get(),
                                           numOfThreads, // more threads produce more files
                                           sizeOfQueue);


            // merging
            size_t levelSize = mergedSegmentsQuee.size();
            for(size_t level = 0; level < levelSize; level++) {
                if (mergedSegmentsQuee[level].empty())
                    continue;
                if (level == 0 || mergedSegmentsQuee[level].size() >= sizeOfQueue) {
                    cout << "Merging level " << level << " queue" << endl;
                    mergeSegments(mergedSegmentsQuee[level], buf.get(), bufSize, numOfThreads);
                    if (!mergedSegmentsQuee[level].empty()) {
                        string merged = mergedSegmentsQuee[level].front();
                        mergedSegmentsQuee[level].pop_front();
                        if (level == levelSize - 1) {
                            mergedSegmentsQuee.push_back(list<string>());
                        }
                        mergedSegmentsQuee[level+1].push_back(merged);
                    }
                }
            }

        }

        input.reset(); // closing input file

        // final  merging
        size_t levelSize = mergedSegmentsQuee.size();
        for(size_t level = 0; level < levelSize; level++) {
            if (mergedSegmentsQuee[level].empty())
                continue;
            if (mergedSegmentsQuee[level].size() >= 1) {
                cout << "Merging level " << level << " queue" << endl;
                mergeSegments(mergedSegmentsQuee[level], buf.get(), bufSize, numOfThreads);
                if (!mergedSegmentsQuee[level].empty() && level < levelSize - 1) {
                    string merged = mergedSegmentsQuee[level].front();
                    mergedSegmentsQuee[level].pop_front();
                    mergedSegmentsQuee[level+1].push_back(merged);
                }
            }
        }

        //buf.reset(); // release bufer if no need


        if (!mergedSegmentsQuee[levelSize-1].empty() &&
                0 != rename(mergedSegmentsQuee[levelSize-1].front().c_str(), outputFile)) {
            cout << "Failed to create output file " << outputFile << endl;
        }

        chrono::steady_clock::time_point endSort = chrono::steady_clock::now();
        auto sortDurationMs = chrono::duration_cast<chrono::milliseconds>(endSort - start).count();

        cout << "Sorting took "
                  << sortDurationMs
                  << "ms." << endl;
    }
    catch (exception& ex) {
        cerr << ex.what() << endl;
        clearTempFiles(s_sortedSegments);
        for (size_t level = 0; level < mergedSegmentsQuee.size(); level++)
            clearTempFiles(mergedSegmentsQuee[level]);
        return 3;
    }
    catch (...) {
        cerr << "Internal error : unhandled exception" << endl;
        clearTempFiles(s_sortedSegments);
        for (size_t level = 0; level < mergedSegmentsQuee.size(); level++)
            clearTempFiles(mergedSegmentsQuee[level]);
        return 4;
    }
    return 0;
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
    uint32_t (*counters)[kMax24bitValue + 1];
    counters = (decltype(counters))countersBuf;


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
            (*counters)[val] += 1;
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
        if ((*counters)[i] == 0) {
            continue;
        }
        count = (*counters)[i];
        (*counters)[i] = 0;
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

void sortAndWrite(ofstream& out, string inputName, uint32_t msb,
                    char* ioBuf, size_t ioBufSize)
{
    cout << "Sorting for msb " << msb << ", file " << inputName << endl;
    ifstream inp(inputName, ios_base::binary | ios_base::in);
    if (!inp.is_open()) {
        cerr << "Failed to open " << inputName << " file." << endl;
        return;
    }


    uint32_t* inputValues = (uint32_t*)ioBuf;
    streamsize read = inp.read(ioBuf, ioBufSize).gcount() / sizeof(uint32_t);
    if (read == 0) {
        return;
    }

    sortParralel(inputValues, read);
    //sort(inputValues, inputValues + read);

    out.write(ioBuf, read*sizeof(uint32_t));
}

int sortByBuckets(const char* inputFile, const char* outputFile)
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

    constexpr size_t kIoBufSize =  16 * kKilobyte;
    unique_ptr<char[]> ioBuf = allocateMaxBuffer(kIoBufSize, kIoBufSize,  &tmp);
    if (!ioBuf) {
        cout << "Not enough RAM to sort file." << endl;
        return 2;
    }
    cout << kIoBufSize << " bytes allocated for io buffer" << endl;


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
    uint64_t addCountersRes[256];
    memset(addCounters, 0, sizeof(addCounters));
    memset(addCountersRes, 0, sizeof(addCountersRes));


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
                addCountersRes[msByte] += addCounters[msByte];
                addCounters[msByte] = 0;
            }
        }
    }
    for (uint32_t msByte = 0; msByte < 256u; msByte++) {
        streams[msByte].write((char*)(*writeBuffers)[msByte], sizeof(uint32_t) * addCounters[msByte]);
        addCountersRes[msByte] += addCounters[msByte];
    }
    for (size_t i = 0; i < tmpNames.size(); i++) {
        streams[i].close();
    }

    chrono::steady_clock::time_point endSplit = chrono::steady_clock::now();


    constexpr size_t kSortCount = kMegabyte*4;
    constexpr size_t kSortBufSize =  kSortCount * sizeof(uint32_t);
    unique_ptr<char[]> sortBuf = allocateMaxBuffer(kSortBufSize, kSortBufSize,  &tmp);
    if (!sortBuf) {
        cout << "Not enough RAM to sort file." << endl;
        return 2;
    }
    cout << kSortBufSize << " bytes allocated for read buffer" << endl;

    ofstream out(outputFile, ios_base::binary | ios_base::out | ios_base::trunc);
    memset(buf.get(), 0, kBufSize);
    for (size_t i = 0; i < tmpNames.size(); i++) {
        if (addCountersRes[i] > kSortCount)
            countAndWrite(out, tmpNames[i], i, buf.get(), ioBuf.get(), kIoBufSize);
        else
            sortAndWrite(out, tmpNames[i], i, sortBuf.get(), kSortBufSize);
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
    int res = sortByBuckets("input", "output");
    //int res = sortFileStepByStep("input", "output_res", 112*kMegabyte, 2, 1024);
    return res;
}
