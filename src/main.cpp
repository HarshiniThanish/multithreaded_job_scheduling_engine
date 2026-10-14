#include "job_scheduler.hpp"
#include <algorithm>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cstring>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <regex>

static std::string esc(const std::string& s) {
    std::string o;
    for(char c:s) { if(c=='"'||c=='\\') o+='\\'; if(c=='\n') {o+="\\n";continue;} if(c!='\r') o+=c; }
    return o;
}
Scheduler::Scheduler(std::size_t n, const std::string& path) {
    if(sqlite3_open(path.c_str(), &db_) != SQLITE_OK) throw std::runtime_error("Could not open SQLite database");
    const char* sql="CREATE TABLE IF NOT EXISTS jobs(id INTEGER PRIMARY KEY,name TEXT,priority INTEGER,status TEXT,created_at TEXT,updated_at TEXT,error TEXT);";
    sqlite3_exec(db_,sql,nullptr,nullptr,nullptr);
    for(std::size_t i=0;i<std::max<std::size_t>(1,n);++i) workers_.emplace_back(&Scheduler::worker_loop,this);
}
Scheduler::~Scheduler(){ stop(); if(db_) sqlite3_close(db_); }
void Scheduler::persist(const Job& j,const std::string& status,const std::string& error) {
    std::lock_guard<std::mutex> lock(db_mutex_);
    sqlite3_stmt* st=nullptr;
    sqlite3_prepare_v2(db_,"INSERT INTO jobs(id,name,priority,status,created_at,updated_at,error) VALUES(?,?,?,?,datetime('now'),datetime('now'),?) ON CONFLICT(id) DO UPDATE SET status=excluded.status,updated_at=datetime('now'),error=excluded.error;",-1,&st,nullptr);
    sqlite3_bind_int64(st,1,j.id); sqlite3_bind_text(st,2,j.name.c_str(),-1,SQLITE_TRANSIENT);
    sqlite3_bind_int(st,3,j.priority); sqlite3_bind_text(st,4,status.c_str(),-1,SQLITE_TRANSIENT);
    sqlite3_bind_text(st,5,error.c_str(),-1,SQLITE_TRANSIENT); sqlite3_step(st); sqlite3_finalize(st);
}
std::int64_t Scheduler::submit(std::string name,int priority,int failures) {
    std::lock_guard<std::mutex> lock(mutex_);
    if(stopping_) throw std::runtime_error("scheduler is stopping");
    Job j{next_id_++,std::move(name),priority,2,std::max(0,failures)};
    ++submitted_; persist(j,"queued"); queue_.push(j); cv_.notify_one(); return j.id;
}
void Scheduler::worker_loop() {
    for(;;) {
        Job j;
        { std::unique_lock<std::mutex> lock(mutex_); cv_.wait(lock,[&]{return stopping_||!queue_.empty();});
          if(stopping_&&queue_.empty()) return;
          j=queue_.top(); queue_.pop(); ++running_; }
        persist(j,"running");
        auto start=std::chrono::steady_clock::now();
        std::this_thread::sleep_for(std::chrono::milliseconds(150+(j.id%5)*80));
        bool fail=j.simulated_failures>0;
        if(fail) {
            --j.simulated_failures;
            if(j.retries_left>0) {
                --j.retries_left;
                { std::lock_guard<std::mutex> lock(mutex_); ++retried_; --running_; queue_.push(j); }
                persist(j,"retrying","simulated failure"); cv_.notify_one(); continue;
            }
            { std::lock_guard<std::mutex> lock(mutex_); ++failed_; --running_; }
            persist(j,"failed","retry limit exceeded");
        } else {
            auto ms=std::chrono::duration<double,std::milli>(std::chrono::steady_clock::now()-j.created_at).count();
            { std::lock_guard<std::mutex> lock(mutex_); ++succeeded_; --running_; latencies_.push_back(ms); }
            persist(j,"succeeded");
        }
    }
}
std::vector<std::string> Scheduler::jobs_json() {
    std::lock_guard<std::mutex> lock(db_mutex_);
    std::vector<std::string> out; sqlite3_stmt* st=nullptr;
    sqlite3_prepare_v2(db_,"SELECT id,name,priority,status,created_at,updated_at,error FROM jobs ORDER BY id DESC LIMIT 100",-1,&st,nullptr);
    while(sqlite3_step(st)==SQLITE_ROW) {
        auto txt=[&](int c){const unsigned char* p=sqlite3_column_text(st,c);return p?std::string((const char*)p):"";};
        std::ostringstream s; s<<"{\"id\":"<<sqlite3_column_int64(st,0)<<",\"name\":\""<<esc(txt(1))<<"\",\"priority\":"<<sqlite3_column_int(st,2)<<",\"status\":\""<<esc(txt(3))<<"\",\"created_at\":\""<<esc(txt(4))<<"\",\"updated_at\":\""<<esc(txt(5))<<"\",\"error\":\""<<esc(txt(6))<<"\"}";
        out.push_back(s.str());
    } sqlite3_finalize(st); return out;
}
JobStats Scheduler::stats() {
    std::lock_guard<std::mutex> lock(mutex_);
    JobStats s; s.submitted=submitted_;s.succeeded=succeeded_;s.failed=failed_;s.retried=retried_;s.queued=queue_.size();s.running=running_;
    if(!latencies_.empty()) { auto v=latencies_; std::sort(v.begin(),v.end()); double sum=0;for(double x:v)sum+=x;s.avg_latency_ms=sum/v.size();s.p95_latency_ms=v[std::min(v.size()-1,(std::size_t)std::ceil(v.size()*0.95)-1)];}
    return s;
}
void Scheduler::stop() {
    {std::lock_guard<std::mutex> lock(mutex_);if(stopping_)return;stopping_=true;}
    cv_.notify_all();for(auto& t:workers_)if(t.joinable())t.join();workers_.clear();
}
static std::string response(int code,const std::string& body,const std::string& type="application/json") {
    std::string status=code==200?"OK":code==201?"Created":code==400?"Bad Request":code==404?"Not Found": "Internal Server Error";
    std::ostringstream s;s<<"HTTP/1.1 "<<code<<" "<<status<<"\r\nContent-Type: "<<type<<"\r\nAccess-Control-Allow-Origin: *\r\nAccess-Control-Allow-Headers: Content-Type\r\nAccess-Control-Allow-Methods: GET, POST, OPTIONS\r\nConnection: close\r\nContent-Length: "<<body.size()<<"\r\n\r\n"<<body;return s.str();
}
int main() {
    try {
        Scheduler scheduler(4,"jobs.db");
        int server=socket(AF_INET,SOCK_STREAM,0);int yes=1;setsockopt(server,SOL_SOCKET,SO_REUSEADDR,&yes,sizeof(yes));
        sockaddr_in addr{};addr.sin_family=AF_INET;addr.sin_addr.s_addr=htonl(INADDR_LOOPBACK);addr.sin_port=htons(8080);
        if(bind(server,(sockaddr*)&addr,sizeof(addr))<0||listen(server,32)<0){std::cerr<<"Cannot bind localhost:8080\n";return 1;}
        std::cout<<"Job scheduler API: http://localhost:8080\n";
        for(;;) {
            int client=accept(server,nullptr,nullptr);if(client<0)continue;
            char buf[16384];int n=recv(client,buf,sizeof(buf)-1,0);if(n<=0){close(client);continue;}buf[n]=0;
            std::string req(buf);std::istringstream in(req);std::string method,path;in>>method>>path;
            std::string out;
            if(method=="OPTIONS") out=response(200,"{}");
            else if(method=="GET"&&path=="/health") out=response(200,"{\"status\":\"ok\"}");
            else if(method=="GET"&&path=="/stats") {
                auto s=scheduler.stats();std::ostringstream j;j<<std::fixed<<std::setprecision(2)<<"{\"submitted\":"<<s.submitted<<",\"succeeded\":"<<s.succeeded<<",\"failed\":"<<s.failed<<",\"retried\":"<<s.retried<<",\"queued\":"<<s.queued<<",\"running\":"<<s.running<<",\"avg_latency_ms\":"<<s.avg_latency_ms<<",\"p95_latency_ms\":"<<s.p95_latency_ms<<"}";
                out=response(200,j.str());
            } else if(method=="GET"&&path=="/jobs") {
                auto rows=scheduler.jobs_json();std::string b="[";for(std::size_t i=0;i<rows.size();++i){if(i)b+=",";b+=rows[i];}b+="]";out=response(200,b);
            } else if(method=="POST"&&path=="/jobs") {
                auto pos=req.find("\r\n\r\n");std::string body=pos==std::string::npos?"{}":req.substr(pos+4);
                auto get=[&](const std::string& key,const std::string& def){std::regex re("\""+key+"\"\\s*:\\s*(\"([^\"]*)\"|(-?[0-9]+))");std::smatch m;if(std::regex_search(body,m,re))return m[2].matched?m[2].str():m[3].str();return def;};
                try {auto id=scheduler.submit(get("name","sample-job"),std::stoi(get("priority","0")),std::stoi(get("failures","0")));out=response(201,"{\"id\":"+std::to_string(id)+"}");}
                catch(const std::exception& e){out=response(400,std::string("{\"error\":\"")+esc(e.what())+"\"}");}
            } else out=response(404,"{\"error\":\"not found\"}");
            send(client,out.data(),out.size(),0);close(client);
        }
        close(server);
    } catch(const std::exception& e){std::cerr<<e.what()<<"\n";return 1;}
}
