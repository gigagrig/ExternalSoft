#include <algorithm>
#include <atomic>
#include <chrono>
#include <list>
#include <iostream>
#include <memory>
#include <mutex>
#include <vector>
#include <thread>
#include <istream>
#include <fstream>



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
    string name(buf);
    return name;
}

// worker that sorts segments of file INPUT
// sorts until file has been read or maxsegments count reached
void sortingSegmentWorker(uint32_t* buffer, size_t bufferSize, istream* input, int maxSegmentsCount)
{
    try {
        unique_lock<mutex> inputLocker(s_inputMutex, defer_lock);
        unique_lock<mutex> queeLocker(s_inputMutex, defer_lock);
        while (!s_isFileRead.load())
        {
            inputLocker.lock();
            if (s_segmentsCount >= maxSegmentsCount)
                break;
            input->read((char*)buffer, bufferSize*sizeof(uint32_t));
            size_t readCount = input->gcount() / sizeof(uint32_t);
            if (readCount == 0) {
                s_isFileRead.store(true);
                return;
            }
            int current_segment = ++s_segmentsCount;
            inputLocker.unlock();

            sort(buffer, buffer + readCount);
            string chunkName = genUniqueFilename();
            ofstream output(chunkName.c_str(), ios_base::out | ios_base::binary | ios_base::trunc);
            if (!output.is_open()) {
                throw SortAlgException("Failed to create temporary file while sorting segments.");
            }
            output.write((char*)buffer, sizeof(uint32_t) * readCount);
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
list<string> generateSortedSegmentsParallel(uint32_t* sortingBuffer, size_t bufferSize, istream& input,
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
                                &input,
                                maxSegmentsCount);
    for (int i = 0; i < workersCount; ++i)
        if (sortWorkers[i].joinable())
            sortWorkers[i].join();

    if (s_segSortEx) {
        rethrow_exception(s_segSortEx);
    }
    return s_sortedSegments;
}


list<string> generateSortedSegments(uint32_t* sortingBuffer, size_t bufferSize,
                                    ifstream& input, size_t maxSegmentsCount = 64 * 1024)
{
    int chunk = 0;
    list<string> segments;
    while(segments.size() < maxSegmentsCount) {
        input.read((char*)sortingBuffer, bufferSize * sizeof(uint32_t));
        size_t readCount = input.gcount() / sizeof(uint32_t);
        if (readCount == 0) {
            return segments;
        }
        //sort(sortingBuffer, sortingBuffer + readCount);
        thread s1(sort<uint32_t*>, sortingBuffer, sortingBuffer + readCount / 4);
        thread s2(sort<uint32_t*>, sortingBuffer + readCount / 4, sortingBuffer + 2*readCount / 4);
        thread s3(sort<uint32_t*>, sortingBuffer + 2*readCount / 4, sortingBuffer + 3*readCount / 4);
        sort<uint32_t*>(sortingBuffer + 3*readCount / 4, sortingBuffer + readCount);
        s1.join();
        s2.join();
        s3.join();
        s1 = thread(inplace_merge<uint32_t*>, sortingBuffer, sortingBuffer + readCount / 4, sortingBuffer + 2*readCount / 4);
        //inplace_merge<uint32_t*>(sortingBuffer, sortingBuffer + readCount / 4, sortingBuffer + 2*readCount / 4);
        inplace_merge(sortingBuffer + 2*readCount / 4, sortingBuffer + 3*readCount / 4, sortingBuffer + readCount);
        s1.join();
        inplace_merge(sortingBuffer, sortingBuffer + readCount / 2, sortingBuffer + readCount);

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

list<string> genSortedSegments(uint32_t* sortingBuffer, size_t bufferSize, ifstream& input,
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
    ifstream f1(file1.c_str(), ios_base::in | ios_base::binary);
    ifstream f2(file2.c_str(), ios_base::in | ios_base::binary);
    if (!f1.is_open() || !f2.is_open())
        throw SortAlgException("Failed to read temporary file while merging");

    ofstream output(resultName.c_str(), ios_base::out | ios_base::binary | ios_base::trunc);
    if (!output.is_open()) {
        throw SortAlgException("Failed to create temporary file while merging");
    }

    char* rBuf1 = buf;
    size_t readBufSize = bufBytes / 4;
    char* rBuf2 = buf + readBufSize;
    uint32_t* outbuf = (uint32_t*)(buf + bufBytes / 2);

    size_t merged = 0;
    uint32_t* it1 = 0;
    uint32_t* it2 = 0;

    f1.read(rBuf1, bufBytes / 4);
    f2.read(rBuf2, bufBytes / 4);
    streamsize read1 = f1.gcount() / sizeof(uint32_t);
    streamsize read2 = f2.gcount() / sizeof(uint32_t);


    while (read1 > 0 && read2 > 0) {
        if (it1 == 0)
            it1 = (uint32_t*)rBuf1;
        if (it2 == 0)
            it2 = (uint32_t*)rBuf2;
        while (read1 > 0 && read2 > 0) {
            if (*it1 <= *it2) {
                outbuf[merged] = *it1;
                read1--;
                it1++;
                merged++;
            } else {
                outbuf[merged] = *it2;
                read2--;
                it2++;
                merged++;
            }
        }
        output.write((char*)outbuf, sizeof(uint32_t) * merged);
        merged = 0;

        if (read1 == 0) {
            f1.read(rBuf1, readBufSize);
            read1 = f1.gcount() / sizeof(uint32_t);
            it1 = 0;
        }
        if (read2 == 0) {
            f2.read(rBuf2, readBufSize);
            read2 = f2.gcount() / sizeof(uint32_t);
            it2 = 0;
        }
    }

    if (read1 > 0 || read2 > 0) {
        uint32_t* it = read1 > 0 ? it1 : it2;
        size_t count = read1 > 0 ? read1 : read2;
        output.write((char*)it, count * sizeof(uint32_t));
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
            *size = (*size) / 2;
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

        ifstream input(inputFile, ios_base::in | ios_base::binary);

        if (!input.is_open()) {
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
                                       input,
                                       numOfThreads);


        input.close(); // closing input file

        chrono::steady_clock::time_point endSort = chrono::steady_clock::now();

        auto sortDurationMs = chrono::duration_cast<chrono::milliseconds>(endSort - start).count();

        //merging
        start = chrono::steady_clock::now();

        mergeSegments(segments, buf.get(),  kKilobyte * 512, 1);

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

        ifstream input(inputFile, ios_base::in | ios_base::binary);

        if (!input.is_open()) {
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

        while (input.good()) {
            mergedSegmentsQuee[0] = genSortedSegments(reinterpret_cast<uint32_t*>(buf.get()),
                                           bufSize / sizeof(uint32_t),
                                           input,
                                           numOfThreads, // more threads produce more files
                                           sizeOfQueue);


            // merging
            size_t levelSize = mergedSegmentsQuee.size();
            for(size_t level = 0; level < levelSize; level++) {
                if (mergedSegmentsQuee[level].empty())
                    continue;
                if (level == 0 || mergedSegmentsQuee[level].size() >= sizeOfQueue) {
                    cout << "Merging level " << level << " queue" << endl;
                    mergeSegments(mergedSegmentsQuee[level], buf.get(), kKilobyte * 512, numOfThreads);
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

        input.close(); // closing input file

        // final  merging
        size_t levelSize = mergedSegmentsQuee.size();
        for(size_t level = 0; level < levelSize; level++) {
            if (mergedSegmentsQuee[level].empty())
                continue;
            if (mergedSegmentsQuee[level].size() >= 1) {
                cout << "Merging level " << level << " queue" << endl;
                mergeSegments(mergedSegmentsQuee[level], buf.get(), kKilobyte * 512, numOfThreads);
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

int sortFileByCounting(const char* inputFile, const char* outputFile,
                        size_t maxBufferSize)
{
    chrono::steady_clock::time_point start = chrono::steady_clock::now();

    ifstream in;
    in.open(inputFile, ios_base::in | ios_base::binary);
    ofstream out;
    out.open(outputFile, ios_base::out | ios_base::binary | ios_base::trunc);

    if (!in.is_open()) {
        cerr << "Failed to open INPUT file." << endl;
        return 1;
    }
    if (!out.is_open()) {
        cerr << "Failed to open output file." << endl;
        return 1;
    }


    size_t bufSize;
    unique_ptr<char[]> buf = allocateMaxBuffer(maxBufferSize, kKilobyte, &bufSize);

    unique_ptr<char[]> bufOut = allocateMaxBuffer(maxBufferSize, kKilobyte, &bufSize);

    in.read(buf.get(), bufSize);
    uint32_t* b1 = (uint32_t*)buf.get();
    uint32_t* b2 = (uint32_t*)bufOut.get();
    while (in.gcount() / 4 > 0) {
        for(int i = 0; i < in.gcount() / 4; ++i) {
            b2[i] = b1[i];
        }
        out.write(bufOut.get(), 4 * (in.gcount() / 4));
        in.read(buf.get(), bufSize);
    }

    chrono::steady_clock::time_point endSort = chrono::steady_clock::now();
    auto sortDurationMs = chrono::duration_cast<chrono::milliseconds>(endSort - start).count();

    cout << "Sorting took "
              << sortDurationMs
              << "ms." << endl;


    return 0;

}

int main()
{
    //int res = sortFileIn2Steps("input", "output", kMegabyte*112, 4);
    int res = sortFileStepByStep("input", "output", kMegabyte*128, 4, 1000);
    return res;
}
