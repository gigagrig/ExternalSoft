#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <map>
#include <memory>
#include <map>
#include <mutex>
#include <thread>
#include <queue>


constexpr size_t kSizeOfBuf = 1024 * 1024 * 8 / 2; // 8*8 = 256 мб.

using namespace std;

/// Possible exceptions on sorting process
class SortAlgException : exception {
public:
    SortAlgException(const string& what) : exception() {
        m_what = what;
    }
    const char* what() const noexcept override { return m_what.c_str(); }
private:
    string m_what;
};

struct sortedSegment
{
    string filename;
    int level;
};

queue<sortedSegment> sortedFiles;

typedef unique_ptr<FILE,  function<void(FILE*)>> UniqueFileHandler;
UniqueFileHandler makeUnique(const char* fileName, const char* mode)
{
    return unique_ptr<FILE, function<void(FILE*)>>(fopen(fileName, mode), [fileName](FILE* f)
        {
            cout << fileName << " closed\n";
            fclose(f);
        });
}


mutex inputMutex;
atomic_bool isFileRead;
atomic_int segmentsCount;
static exception_ptr segSortEx = nullptr;
static exception_ptr segMergeEx = nullptr;
atomic_bool merged;
mutex queeMutex;

string genUniqueFilename() {
    char nameBuf[L_tmpnam];
    tmpnam(nameBuf);
    return string(nameBuf);
}


template<typename T>
void sortingSegmentWorker(T* buffer, int bufferSize, FILE* input)
{
    segmentsCount.store(0);
    try {
        unique_lock<mutex> inp(inputMutex, defer_lock);
        unique_lock<mutex> quee(inputMutex, defer_lock);
        while (!isFileRead.load())
        {
            inp.lock();
            size_t readCount = fread(buffer, sizeof(T), bufferSize, input);
            inp.unlock();
            if (readCount == 0) {
                isFileRead.store(true);
                return;
            }

            sort(buffer, buffer + bufferSize);
            string chunkName = genUniqueFilename();
            auto output = makeUnique(chunkName.c_str(), "wb+");
            if (!output) {
                throw SortAlgException("Failed to create temporary file while sorting segments.");
            }
            fwrite(buffer, sizeof(T), readCount, output.get());
            quee.lock();
            sortedFiles.push({string(chunkName), 0});
            cout << "chunk " << segmentsCount.fetch_add(1) + 1 << " sorted\n";
            quee.unlock();
        }
    }
    catch(...) {
        isFileRead.store(true);
        segSortEx = current_exception();
    }

}

template<typename T>
void generateSortedSegmentsParallel(T* sortingBuffer, int bufferSize, FILE* input) {

    constexpr int kWorkersCount = 4;
    thread sortWorkers[kWorkersCount];
    isFileRead.store(false);
    const int  workerBufSize = bufferSize / kWorkersCount;
    for (int i = 0; i < kWorkersCount; ++i)
        sortWorkers[i] = thread(sortingSegmentWorker<T>, sortingBuffer + i*workerBufSize, workerBufSize, input);
    for (int i = 0; i < kWorkersCount; ++i)
        if (sortWorkers[i].joinable())
            sortWorkers[i].join();

    if (segSortEx) {
        rethrow_exception(segSortEx);
    }
}


template<typename T>
void generateSortedSegments(T* sortingBuffer, int bufferSize, FILE* input)
{
    int chunk = 0;
    while(!feof(input)) {
        size_t readCount = fread(sortingBuffer, sizeof(T), bufferSize, input);
        if (readCount == 0) {
            return;
        }
        sort(sortingBuffer, sortingBuffer + bufferSize);
        cout << "chunk " << ++chunk << " sorted\n";
        const char *chunkName =  tmpnam(0);
        auto output = makeUnique(chunkName, "wb+");
        if (!output) {
            throw SortAlgException("Failed to create temporary file while sorting segments."); //
        }
        fwrite(sortingBuffer, sizeof(T),  readCount, output.get());
        sortedFiles.push({string(chunkName), 0});
    }
}

template<typename T>
void mergeSortedFilesPair(const string& file1, const string& file2, const string& resultName)
{
    cout << "Merging " << file1 << " and " << file2 << " to " << resultName << "\n";
    auto f1 = makeUnique(file1.c_str(), "rb");
    auto f2 = makeUnique(file2.c_str(), "rb");
    if (!f1 || !f2)
        throw SortAlgException("Failed to read temporary file while merging");
    auto output = makeUnique(resultName.c_str(), "wb+");

    FILE* outDesc = output.get();
    if (!output) {
        throw SortAlgException("Failed to create temporary file while merging");
    }

    T vals[2];
    FILE* descriptors[2] {f1.get(), f2.get()};
    fread(vals, sizeof(T), 1, descriptors[0]);
    fread(vals+1, sizeof(T), 1, descriptors[1]);

    unsigned nextIdx = 0;
    for(;;) {
        nextIdx = 0;
        if (vals[1] < vals[0])
            nextIdx = 1;
        fwrite(vals + nextIdx, sizeof(T), 1, outDesc);
        size_t read = fread(vals + nextIdx, sizeof(T), 1, descriptors[nextIdx]);
        if (read == 0) {
            break;
        }
    }

    nextIdx = (nextIdx + 1) % 2;
    fwrite(vals + nextIdx, sizeof(T), 1, outDesc);
    while (0 < fread(vals + nextIdx, sizeof(T), 1, descriptors[nextIdx])) {
        fwrite(vals + nextIdx, sizeof(T), 1, outDesc);
    }
}

constexpr size_t kBufSize = 64*1024;

template<class T> class MinStream
{
public:
    MinStream(const vector<string>& filenames) {
        try {
            m_filenames = filenames;

            m_handlers.resize(filenames.size());
            for (int i = 0; i < m_filenames.size(); ++i) {
                m_handlers[i] =  makeUnique(m_filenames[i].c_str(), "rb");
                m_bufs.push_back(unique_ptr<char[]>(new char[kBufSize]));
                setvbuf(m_handlers[i].get(), m_bufs[i].get(), _IOFBF, kBufSize);
                if (!m_handlers[i]) {
                    throw SortAlgException("Failed to read temporary file while merging");
                }
                T initValue;
                fread(&initValue, sizeof(T), 1, m_handlers[i].get());
                m_streamMap.insert({initValue, m_handlers[i].get()});
            }
        }
        catch(...) {
            m_streamMap.~multimap();
            m_handlers.~vector();
            m_filenames.~vector();
            m_bufs.~vector();
            throw;
        }
    }

    T getNextValue() {
        auto begin = m_streamMap.begin();
        T next = begin->first;
        FILE* f = begin->second;
        m_streamMap.erase(begin);
        T newNext;
        if (fread(&newNext, sizeof(T), 1, f) == 1) {
            m_streamMap.insert({newNext, f});
        }
        return next;
    }
    int openStreamsCount() const { return m_streamMap.size(); }
private:
    std::vector<unique_ptr<char[]>> m_bufs;
    vector<string> m_filenames;
    vector<UniqueFileHandler> m_handlers;
    multimap<T, FILE*> m_streamMap;
};

unique_ptr<char[]> outBuf(new char[kBufSize]);

template<typename T>
void mergeSortedFiles(const vector<string>& files, const string& resultName)
{
    MinStream<T> valuesStream(files);
    auto output = makeUnique(resultName.c_str(), "wb+");

    FILE* outDesc = output.get();
    if (!output) {
        throw SortAlgException("Failed to create temporary file for merging");
    }

    setvbuf(outDesc, outBuf.get(), _IOFBF, kBufSize);

    cout << "memory allocated\n";
    while (valuesStream.openStreamsCount() > 0) {
        T val = valuesStream.getNextValue();
        fwrite(&val, sizeof(T), 1, outDesc);
    }
}


template<typename T>
void mergeSortedFileWork(int targetLevel)
{
    try {
        while(!merged.load()) {

            // fetching next files to merge
            sortedSegment f1;
            sortedSegment f2;
            unique_lock<mutex> l(queeMutex, defer_lock);
            l.lock();
            if (sortedFiles.size() < 2) {
                continue;
            }
            f1 = sortedFiles.front();
            sortedFiles.pop();
            f2 = sortedFiles.front();
            sortedFiles.pop();
            l.unlock();

            // merging to resfile
            string resFile;
            int nextLevel = max(f1.level, f2.level) + 1;
            if (nextLevel == targetLevel) {
                merged.store(true);
            }
            resFile = nextLevel < targetLevel ? string(tmpnam(0)) : string("OUTPUT");
            mergeSortedFilesPair<T>(f1.filename, f2.filename, resFile);

            l.lock();
            sortedFiles.push(sortedSegment{resFile, nextLevel});
            l.unlock();

            // removing temp files
            if (0 != remove(f1.filename.c_str())) {
                cout << "FAILED TO REMOVE FILE " << f1.filename << endl;
            }
            if (0 != remove(f2.filename.c_str())) {
                cout << "FAILED TO REMOVE FILE " << f2.filename << endl;
            }

            if (nextLevel == targetLevel) {
                break;
            }
        }
    }
    catch(...)
    {
        merged.store(true);
        segMergeEx = current_exception();
        cout << "Thread " << this_thread::get_id() << " throwed exception\n";
        rethrow_exception(segMergeEx);
    }
    cout << "Thread " << this_thread::get_id() << " finished\n";
}

template<typename T>
void mergeSegmentsParallel()
{
    int targetLevel = ceil(log2(sortedFiles.size()));
    constexpr int kPoolSize = 4;
    thread mergePool[kPoolSize];
    merged.store(false);
    for (int i = 0; i < kPoolSize; ++i)
        mergePool[i] = thread(mergeSortedFileWork<T>, targetLevel);
    for (int i = 0; i < kPoolSize; ++i)
        if (mergePool[i].joinable())
            mergePool[i].join();
    if (segMergeEx) {
        rethrow_exception(segMergeEx);
    }

}


template<typename T>
void mergeSegmentsByPairs()
{
    int targetLevel = ceil(log2(sortedFiles.size()));
    while (sortedFiles.size() > 1) {
        sortedSegment f1(sortedFiles.front());
        sortedFiles.pop();
        sortedSegment f2(sortedFiles.front());
        sortedFiles.pop();
        string resFile;
        int nextLevel = max(f1.level, f2.level) + 1;
        resFile = nextLevel < targetLevel ? string(tmpnam(0)) : string("OUTPUT");
        mergeSortedFilesPair<T>(f1.filename, f2.filename, resFile);
        sortedFiles.push(sortedSegment{resFile, nextLevel});
        remove(f1.filename.c_str());
        remove(f2.filename.c_str());
    }
}

template<typename T>
void mergeSegmentsDirectly()
{
    const size_t kMaxOpenFilesCount = 64;

    while (sortedFiles.size() > 1) {
        size_t size = min(kMaxOpenFilesCount, sortedFiles.size());
        std::vector<string> files;
        for (size_t i = 0; i < size; ++i) {
            sortedSegment f1(sortedFiles.front());
            sortedFiles.pop();
            files.push_back(f1.filename);
        }
        string resFile;
        //int nextLevel = max(f1.level, f2.level) + 1;
        resFile = sortedFiles.size() > 0 ? string(tmpnam(0)) : string("OUTPUT");
        mergeSortedFiles<T>(files, resFile);
        sortedFiles.push(sortedSegment{resFile, 0});
        for (size_t i = 0; i < files.size(); ++i) {
            remove(files[i].c_str());
        }
    }
}


void clearTempFiles()
{
    while (sortedFiles.size() > 0) {
        cout << "Removing temp file " << sortedFiles.front().filename.c_str() << endl;
        remove(sortedFiles.front().filename.c_str());
        sortedFiles.pop();
    }
}

template<typename T>
bool checkIsSorted(const char* filename)
{
    unique_ptr<FILE,  function<void(FILE*)>> input = makeUnique(filename, "rb");
    if (!input) {
        cout << "Failed to open file " << filename << endl;
        return false;
    }
    FILE* inputRaw = input.get();
    T current;
    if (0 == fread(&current, sizeof(T), 1, inputRaw)) {
        return true;
    }
    T next;
    while(true) {
        if (0 == fread(&next, sizeof(T), 1, inputRaw)) {
            return true;
        }
        if (next < current)
            return false;
        current = next;
    }
}


int main()
{
    cout << "Hello Artec!" << endl;

    const char* intputFile = "INPUT2";

    try {

        chrono::steady_clock::time_point start = chrono::steady_clock::now();

        unique_ptr<FILE,  function<void(FILE*)>> input = makeUnique(intputFile, "rb");

        if (!input) {
            cerr << "Failed top open INPUT file.\n";
            return 1;
        }


        unique_ptr<uint32_t[]> p(new uint32_t[kSizeOfBuf]);
        if (!p)
            throw bad_alloc();

        //generateSortedSegments(p.get(), kSizeOfBuf, input.get());
        generateSortedSegmentsParallel(p.get(), kSizeOfBuf, input.get());
        input.reset(); // closing input file

        chrono::steady_clock::time_point endSort = chrono::steady_clock::now();

        auto sortDurationMs = chrono::duration_cast<chrono::milliseconds>(endSort - start).count();

        //merging
        start = chrono::steady_clock::now();
        //mergeSegmentsByPairs<uint32_t>();
        mergeSegmentsParallel<uint32_t>();
        //mergeSegmentsDirectly<uint32_t>();

        chrono::steady_clock::time_point endMerge = chrono::steady_clock::now();

        cout << "Sorting took "
                  << sortDurationMs
                  << "ms.\n";
        cout << "Merging took "
                  << std::chrono::duration_cast<chrono::milliseconds>(endMerge - start).count()
                  << "ms.\n";
    }
    catch (exception& ex) {
        cerr << ex.what() << endl;
        clearTempFiles();
        exit(1);
    }

    cout << "OUTPUT" << " sorted " << (checkIsSorted<uint32_t>("OUTPUT") ? "true" : "false") << endl;

    return 0;
}
