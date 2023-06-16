#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <cmath>
#include <ctime>
#include <cstring>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sstream>

using namespace std;

const string data_folder = "data";
const string index_folder = "index";
const string data_ext = ".csv";
const string index_ext = ".idx";
const string log_file = "log.txt";
const int data_size = sizeof(float) * 4;

struct Data {
    float x;
    float y;
    float z;
    float w;
};

struct IndexRecord {
    float x;
    float y;
    float z;
    float w;
    long offset;
};

struct Window {
    float x_min;
    float x_max;
    float y_min;
    float y_max;
    float z_min;
    float z_max;
    float w_min;
    float w_max;
};

class DataFile {
public:
    DataFile(string database, string filename) : database_(database), filename_(filename), fd_(-1), size_(0) {}

    bool Create() {
        if (Exists()) {
            return false;
        }
        fd_ = open(filename_.c_str(), O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
        log("create", "");
        return fd_ != -1;
    }

    bool Open() {
        if (!Exists()) {
            return false;
        }
        fd_ = open(filename_.c_str(), O_RDWR);
        size_ = lseek(fd_, 0, SEEK_END);
        log("open", "");
        return fd_ != -1;
    }

    bool Close() {
        if (fd_ == -1) {
            return false;
        }
        close(fd_);
        fd_ = -1;
        log("close", "");
        return true;
    }

    bool Write(long offset, const Data& data) {
        if (fd_ == -1 || offset < 0 ) {
            return false;
        }
        lseek(fd_, offset, SEEK_SET);
        write(fd_, &data.x, sizeof(float));
        write(fd_, &data.y, sizeof(float));
        write(fd_, &data.z, sizeof(float));
        write(fd_, &data.w, sizeof(float));
        size_ += sizeof(Data);
        stringstream ss;
        ss << data.x << "," << data.y << "," << data.z << "," << data.w;
        log("write", ss.str());
        return true;
    }

    bool Read(long offset, Data& data) {
        if (fd_ == -1 || offset < 0 || offset + sizeof(Data) > size_) {
            return false;
        }
        char x;
        lseek(fd_, offset, SEEK_SET);
        read(fd_, &data.x, sizeof(float));
        read(fd_, &data.y, sizeof(float));
        read(fd_, &data.z, sizeof(float));
        read(fd_, &data.w, sizeof(float));
        stringstream ss;
        ss << data.x << "," << data.y << "," << data.z << "," << data.w;
        log("read", ss.str());
        return true;
    }

    bool Truncate(long size) {
        if (fd_ == -1) {
            return false;
        }
        return ftruncate(fd_, size) == 0;
    }

    bool Exists() {
        struct stat buffer;
        return stat(filename_.c_str(), &buffer) == 0;
    }

    long Size() {
        return size_;
    }

    bool Restore(string log_filename, string timestamp) {
        ifstream log_file(log_filename);
        if (!log_file.is_open()) {
            return false;
        }
        string line;
        while (getline(log_file, line)) {
            char delimiter = ',';
            vector<string> tokens = split(line, delimiter);
            if (tokens.size() < 4) {
                continue;
            }
            string op_timestamp = tokens[0];
            string op_filename = tokens[1];
            string op_type = tokens[2];
            string op_data = tokens[3];
            if (op_timestamp <= timestamp && op_filename == filename_) {
                if (op_type == "write") {
                    long offset = (log_file.tellg() - line.size() - 1) - size_;
                    stringstream ss(op_data);
                    float x, y, z, w;
                    ss >> x;
                    ss.ignore();
                    ss >> y;
                    ss.ignore();
                    ss >> z;
                    ss.ignore();
                    ss >> w;
                    Data data = {x, y, z, w};
                    Write(offset, data);
                } else if (op_type == "truncate") {
                    long size = stol(op_data);
                    Truncate(size);
                }
            }
        }
        log_file.close();
        return true;
    }

private:
    string filename_;
    string database_;
    int fd_;
    long size_;
    void log(string op_type, string op_data) {
        ofstream log_file(database_+"/data/"+"log.txt", ios::app);
        time_t current_time = time(nullptr);
        tm* local_time = localtime(&current_time);
        char timestamp[20];
        strftime(timestamp, 20, "%F:%T", local_time);
        log_file << timestamp << "," << filename_ << "," << op_type << "," << op_data << endl;
        log_file.close();
    }

    vector<string> split(const string& s, const char delimiter) {
        vector<string> tokens;
        stringstream ss(s);
        string token;
        while (getline(ss, token, delimiter)) {
            tokens.push_back(token);
        }
        return tokens;
    }
};

class IndexFile {
public:
    IndexFile(string database, string filename) : database_(database), filename_(filename), fd_(-1), size_(0) {}

    int get_fd(){
        return fd_;
    }
    bool Create() {
        if (Exists()) {
            return false;
        }
        fd_ = open(filename_.c_str(), O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
        log("create", "");
        return fd_ != -1;
    }

    bool Open() {
        if (!Exists()) {
            return false;
        }
        fd_ = open(filename_.c_str(), O_RDWR);
        size_ = lseek(fd_, 0, SEEK_END);
        log("open", "");
        return fd_ != -1;
    }

    bool Close() {
        if (fd_ == -1) {
            return false;
        }
        close(fd_);
        fd_ = -1;
        log("close", "");
        return true;
    }

    bool Write(long offset, const IndexRecord& record) {
        if (fd_ == -1 || offset < 0) {
            return false;
        }
        lseek(fd_, offset, SEEK_SET);
        write(fd_, &record.x, sizeof(float));
        write(fd_, &record.y, sizeof(float));
        write(fd_, &record.z, sizeof(float));
        write(fd_, &record.w, sizeof(float));
        write(fd_, &record.offset, sizeof(long));
        size_ += sizeof(IndexRecord);
        stringstream ss;
        ss << record.x << "," << record.y << "," << record.z << "," << record.w << "," << record.offset;
        log("write", ss.str());
        return true;
    }

    bool Read(long offset,IndexRecord& record) {
        if (fd_ == -1 || offset < 0 || offset + sizeof(IndexRecord) > size_) {
            return false;
        }
        lseek(fd_, offset, SEEK_SET);
        read(fd_, &record.x, sizeof(float));
        read(fd_, &record.y, sizeof(float));
        read(fd_, &record.z, sizeof(float));
        read(fd_, &record.w, sizeof(float));
        read(fd_, &record.offset, sizeof(long));
        stringstream ss;
        ss << record.x << "," << record.y << "," << record.z << "," << record.w << "," << record.offset;
        log("read", ss.str());
        return true;
    }

    bool Truncate(long size) {
        if (fd_ == -1) {
            return false;
        }
        stringstream ss;
        ss << size;
        log("truncate", ss.str());
        return ftruncate(fd_, size) == 0;
    }

    bool Exists() {
        struct stat buffer;
        return stat(filename_.c_str(), &buffer) == 0;
    }

    long Size() {
        return size_;
    }
bool Restore(string log_filename, string timestamp) {
        ifstream log_file(log_filename);
        if (!log_file.is_open()) {
            return false;
        }
        string line;
        while (getline(log_file, line)) {
            char delimiter = ',';
            vector<string> tokens = split(line, delimiter);
            if (tokens.size() < 4) {
                continue;
            }
            string op_timestamp = tokens[0];
            string op_filename = tokens[1];
            string op_type = tokens[2];
            string op_data = tokens[3];
            if (op_timestamp <= timestamp && op_filename == filename_) {
                if (op_type == "write") {
                    long offset = (log_file.tellg() - line.size() - 1) - size_;
                    stringstream ss(op_data);
                    float x, y, z, w;
                    ss >> x;
                    ss.ignore();
                    ss >> y;
                    ss.ignore();
                    ss >> z;
                    ss.ignore();
                    ss >> w;
                    ss.ignore();
                    long data_offset;
                    ss >> data_offset;
                    IndexRecord record = {x, y, z, w, data_offset};
                    Write(offset, record);
                } else if (op_type == "truncate") {
                    long size = stol(op_data);
                    Truncate(size);
                }
            }
        }
        log_file.close();
        return true;
    }

private:
    string filename_;
    string database_;
    int fd_;
    long size_;
    void log(string op_type, string op_data) {
        ofstream log_file(database_+"/index/"+"log.txt", ios::app);
        time_t current_time = time(nullptr);
        tm* local_time = localtime(&current_time);
        char timestamp[20];
        strftime(timestamp, 20, "%F:%T", local_time);
        log_file << timestamp << "," << filename_ << "," << op_type << "," << op_data << endl;
        log_file.close();
    }

    vector<string> split(const string& s, const char delimiter) {
        vector<string> tokens;
        stringstream ss(s);
        string token;
        while (getline(ss, token, delimiter)) {
            tokens.push_back(token);
        }
        return tokens;}
};

class Database {
public:
    Database(string name) : name_(name), data_file_(name, name + "/" + data_folder + "/" + name + data_ext), index_file_(name, name + "/" + index_folder + "/" + name + index_ext) {
        mkdir((name_).c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    }
    Database(string name, string dataname, string indexname) : name_(name), data_file_(name, name + "/" + data_folder + "/" + dataname), index_file_(name, name + "/" + index_folder + "/" + indexname) {
        Create();
    }

    bool ReadData(){
        data_file_.Open();
        long offset = 0;
        Data data;
        data_file_.Read(offset, data);
    }

    bool Create() {
        if (Exists()) {
            return false;
        }
        mkdir((name_ + "/" + data_folder).c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
        mkdir((name_ + "/" + index_folder).c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
        return data_file_.Create() && index_file_.Create();
    }

    bool Open() {
        if(data_file_.Open() && !index_file_.Open()){
            index_file_.Create();
            MakeIndexFile();
        }
        return data_file_.Open() && index_file_.Open();
    }

    bool Close() {
        return data_file_.Close() && index_file_.Close();
    }

    bool Exists() {
        struct stat buffer;
        return stat((name_ + "/" + data_folder).c_str(), &buffer) == 0;
    }

    bool AddData(const Data& data) {
        long offset = data_file_.Size();
        if (!data_file_.Write(offset, data)) {
            return false;
        }
        IndexRecord record = { data.x, data.y, data.z, data.w, offset };
        return index_file_.Write(index_file_.Size(), record);
    }

    bool UpdateData(long offset, const Data& data) {
        if (!data_file_.Write(offset, data)) {
            return false;
        }
        // Update the corresponding index record
        IndexRecord record;
        long index_offset = 0;
        while (index_file_.Read(index_offset, record)) {
            if (record.offset == offset) {
                record.x = data.x;
                record.y = data.y;
                record.z = data.z;
                record.w = data.w;
                index_file_.Write(index_offset, record);
                return true;
            }
            index_offset += sizeof(IndexRecord);
        }
        return false;
    }

    bool DeleteData(long offset) {
        // Delete the data record
        if (!data_file_.Truncate(offset)) {
            return false;
        }
        // Delete the corresponding index record
        IndexRecord record;
        long index_offset = 0;
        while (index_file_.Read(index_offset, record)) {
            if (record.offset == offset) {
                if (!index_file_.Truncate(index_offset)) {
                    return false;
                }
                // Shift the remaining index records
                long remaining_size = index_file_.Size() - index_offset;
                if (remaining_size > 0) {
                    char buffer[remaining_size];
                    lseek(index_file_.get_fd(), index_offset + sizeof(IndexRecord), SEEK_SET);
                    read(index_file_.get_fd(), buffer, remaining_size);
                    lseek(index_file_.get_fd(), index_offset, SEEK_SET);
                    write(index_file_.get_fd(), buffer, remaining_size);
                }
                return true;
            }
            index_offset += sizeof(IndexRecord);
        }
        return false;
    }

    bool MakeIndexFile(){
        long data_offset = 0;
        long index_offset = 0;
        Data data;
        IndexRecord record;
        while(data_file_.Read(data_offset, data)){
            record = {data.x, data.y, data.z, data.w, data_offset};
            index_file_.Write(index_offset,record);
            data_offset += sizeof(Data);
            index_offset += sizeof(IndexRecord);
        }
        return true;
    }

    bool QueryData(const Window& window, vector<Data>& results, int& count) {
        results.clear();
        IndexRecord record;
        long index_offset = 0;
        while (index_file_.Read(index_offset, record)) {
            count++;
            if (record.x >= window.x_min && record.x <= window.x_max &&
                record.y >= window.y_min && record.y <= window.y_max &&
                record.z >= window.z_min && record.z <= window.z_max &&
                record.w >= window.w_min && record.w <= window.w_max) {
                Data data;
                if (data_file_.Read(record.offset, data)) {
                    count++;
                    results.push_back(data);
                }
            }
            index_offset += sizeof(IndexRecord);
        }
        return true;
    }
    bool QueryData_no_index(const Window& window, vector<Data>& results,int& count) {
        results.clear();
        long data_offset = 0;
        Data data;
        while (data_file_.Read(data_offset, data)) {
            count++;
            if (data.x >= window.x_min && data.x <= window.x_max &&
                data.y >= window.y_min && data.y <= window.y_max &&
                data.z >= window.z_min && data.z <= window.z_max &&
                data.w >= window.w_min && data.w <= window.w_max) {
                results.push_back(data);
            }
            data_offset += sizeof(Data);
        }
        return true;
    }
    bool Rollback(string timestamp){
        if(data_file_.Restore(name_+"/data/"+"log.txt", timestamp) && index_file_.Restore(name_+"/index/"+"log.txt", timestamp))
        {
            return true;
        }
        else{
            return false;
        }
    }

private:
    string name_;
    DataFile data_file_;
    IndexFile index_file_;
};

class DataProcessor {
public:
    DataProcessor(string database, string data_filename, string index_filename) :database(database), data_file_(database, database+"/data/"+data_filename), index_file_(database, database+"/index/"+index_filename) {}

    bool Interpolate() {
        if (!data_file_.Open() || !index_file_.Open()) {
            if(!index_file_.Exists())
                {index_file_.Create();}      
        }

        long data_size = data_file_.Size();
        long index_size = index_file_.Size();
        long num_data_records = data_size / sizeof(Data);
        long num_index_records = index_size / sizeof(IndexRecord);
        if (num_data_records != num_index_records) {
            return false;
        }

        vector<Data> data_records;
        vector<IndexRecord> index_records;
        data_records.reserve(num_data_records);
        index_records.reserve(num_index_records);

        for (long i = 0; i < num_data_records; i++) {
            Data data;
            data_file_.Read(i * sizeof(Data), data);
            data_records.push_back(data);

            IndexRecord index;
            index_file_.Read(i * sizeof(IndexRecord), index);
            index_records.push_back(index);
        }

        vector<Data> interpolated_data;
        interpolated_data.reserve(num_data_records);

        for (long i = 0; i < num_data_records; i++) {
            Data data = data_records[i];
            // IndexRecord index = index_records[i];
            if (data.x == 0 && data.y == 0 && data.z == 0 && data.w == 0) {
                Data interpolated = InterpolateData(data_records, index_records, i);
                interpolated_data.push_back(interpolated);
            } else {
                interpolated_data.push_back(data);
            }
        }

        data_file_.Close();
        index_file_.Close();

        // Write the interpolated data to a new data file
        string interpolated_file = "interpolated_data.csv";
        DataFile new_data_file(database, database+"/data/"+interpolated_file);
        if (!new_data_file.Create()) {
            return false;
        }

        for (long i = 0; i < num_data_records; i++) {
            new_data_file.Write(i * sizeof(Data), interpolated_data[i]);
        }

        new_data_file.Close();

        // Validate the interpolated data
        float error_sum = 0.0f;
        for (long i = 0; i < num_data_records; i++) {
            Data original_data = data_records[i];
            Data interpolated_data_i = interpolated_data[i];
            float error = sqrt(pow(original_data.x - interpolated_data_i.x, 2) +
                               pow(original_data.y - interpolated_data_i.y, 2) +
                               pow(original_data.z - interpolated_data_i.z, 2) +
                               pow(original_data.w - interpolated_data_i.w, 2));
            error_sum += error;
        }

        float mean_error = error_sum / num_data_records;
        cout << "Mean interpolation error: " << mean_error << endl;

        return true;
    }
private:
    DataFile data_file_;
    IndexFile index_file_;
    string database;
    float kInterpolationRadius = 0.1f;
    Data InterpolateData(const vector<Data>& data_records, const vector<IndexRecord>& index_records, long index) {
        int num_neighbors = 0;
        float sum_x = 0.0f;
        float sum_y = 0.0f;
        float sum_z = 0.0f;
        float sum_w = 0.0f;

        for (long i = 0; i < index_records.size(); i++) {
            if (i == index) {
                continue;
            }

            IndexRecord neighbor_index = index_records[i];
            float distance = sqrt(pow(index_records[index].x - neighbor_index.x, 2) +
                                  pow(index_records[index].y - neighbor_index.y, 2) +
                                  pow(index_records[index].z - neighbor_index.z, 2) +
                                  pow(index_records[index].w - neighbor_index.w, 2));

            if (distance <= kInterpolationRadius) {
                Data neighbor_data = data_records[neighbor_index.offset / sizeof(Data)];
                sum_x += neighbor_data.x;
                sum_y += neighbor_data.y;
                sum_z += neighbor_data.z;
                sum_w += neighbor_data.w;
                num_neighbors++;
            }
        }

        if (num_neighbors == 0) {
            return {0, 0, 0, 0};
        }

        float interpolated_x = sum_x / num_neighbors;
        float interpolated_y = sum_y / num_neighbors;
        float interpolated_z = sum_z / num_neighbors;
        float interpolated_w = sum_w / num_neighbors;

        return {interpolated_x, interpolated_y, interpolated_z, interpolated_w};
    }
   };

int main() {
    // Create a new database
    Database db("test_db");
    if (!db.Create()) {
        cout << "Failed to create database" << endl;
        return 1;
    }
   
    // Add some data to the database
    srand(time(NULL));
    for (int i = 0; i < 20; i++) {
        float x = (float)rand() / (float)RAND_MAX;
        if(x < 0.3){
            Data data = { 0, 0, 0, 0};
            db.AddData(data);
        }else{
            Data data = { (float)rand() / (float)RAND_MAX, (float)rand() / (float)RAND_MAX, (float)rand() / (float)RAND_MAX, (float)rand() / (float)RAND_MAX };
        db.AddData(data);
        }
    }

    // Query the data within a window and index
    Window window = { 0.2, 0.8, 0.2, 0.8, 0.2, 0.8, 0.2, 0.8 };
    vector<Data> results;
    int count_index = 0;
    int count_no_index = 0;
    if (db.QueryData(window, results, count_index)) {
        cout << "Found " << results.size() << " results:" << endl;
        for (const auto& data : results) {
            cout << "Data: " << data.x << ", " << data.y << ", " << data.z << ", " << data.w << endl;
        }
        cout << "Number of index records read:" << count_index << endl;
    }
    else {
        cout << "Failed to query database" << endl;
    }
    // Query the data within a window and without index
      if (db.QueryData_no_index(window, results, count_no_index)) {
        cout << "Found " << results.size() << " results:" << endl;
        for (const auto& data : results) {
            cout << "Data: " << data.x << ", " << data.y << ", " << data.z << ", " << data.w << endl;
        }
        cout << "Number of data records read:" <<count_no_index << endl;
    }
    else {
        cout << "Failed to query database" << endl;
    }

    // Interpolate the data
    DataProcessor processor("test_db", "test_db.csv", "test_db.idx");
    bool success = processor.Interpolate();
    if (success) {
        cout << "Interpolation successful!" << endl;
    } else {
        cout << "Interpolation failed!" << endl;
    }


    time_t current_time = time(nullptr);
    tm* local_time = localtime(&current_time);
    char timestamp[20];
    strftime(timestamp, 20, "%F:%T", local_time);

    // Update a data record
    long offset = 0;
    Data new_data = { 1.0, 2.0, 3.0, 4.0 };
    if (db.UpdateData(offset, new_data)) {
        cout << "Data record updated" << endl;
    }
    else {
        cout << "Failed to update data record" << endl;
    }

    // Delete a data record
    if (db.DeleteData(offset)) {
        cout << "Data record deleted" << endl;
    }
    else {
        cout << "Failed to delete data record" << endl;
    }

    //Rollback the database
    if (!db.Rollback(timestamp)) {
        cout << "Failed to close database" << endl;
        return 1;
    }

    // Close the database
    if (!db.Close()) {
        cout << "Failed to close database" << endl;
        return 1;
    }
    return 0;
    }