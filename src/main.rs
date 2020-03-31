extern crate fuse;
extern crate libc;
extern crate time;
extern crate redis;
extern crate serde_json;

use std::env;
use std::fs::File;
use std::io::prelude::*;
use std::ffi::OsStr;
use std::os::raw::c_int;
use std::collections::BTreeMap;
use libc::ENOENT;
use time::Timespec;

use serde_json::Value;
use fuse::{FileType, FileAttr, Filesystem, Request, ReplyData, ReplyEmpty, ReplyEntry, ReplyAttr, ReplyDirectory, ReplyCreate, ReplyWrite};
use redis::{Commands, Client};

struct HashFS{
    redis_connection_string: String,
    hash_name: String,
    inodes: BTreeMap<String, u64>,
    attrs: BTreeMap<u64, FileAttr>,
}

struct HashAttr;

impl HashAttr {
    fn new(file_type: FileType, ino: u64, ts: time::Timespec, uid: u32, gid: u32) -> FileAttr {

        FileAttr {
            ino: ino,
            size: 500,
            blocks: 0,
            atime: ts,
            mtime: ts,
            ctime: ts,
            crtime: ts,
            kind: file_type,
            perm: 0o644,
            nlink: 0,
            uid: uid,
            gid: gid,
            rdev: 0,
            flags: 0,
        }
    }
}

impl HashFS {
    fn new(hash_name: String, config_json_file: String) -> HashFS {

        let mut file = File::open(config_json_file).expect("file not found");

        let mut json_string = String::new();
        //file.read_to_string(&mut json_string).expect("something went wrong reading the file");
        file.read_to_string(&mut json_string).unwrap();

        let json_value: Value = serde_json::from_str(&json_string).unwrap();

        println!("Json : host {} and port {}", json_value["store"]["host"].to_string().replace("\"", ""), json_value["store"]["port"].to_string().replace("\"", ""));

        let mut redis_url = "redis://".to_string();
        redis_url.push_str(&json_value["store"]["host"].to_string().replace("\"", ""));
        redis_url.push_str(&json_value["store"]["port"].to_string().replace("\"", ""));
        redis_url.push_str("/");

        println!("Redis URL: {}", redis_url);

        let attrs = BTreeMap::new();
        let inodes = BTreeMap::new();

        HashFS {
            redis_connection_string: redis_url,
            hash_name: hash_name,
            attrs: attrs,
            inodes: inodes,
        }
    }
}

impl Filesystem for HashFS {
   
    fn init(&mut self, req: &Request) -> Result<(), c_int> {
        println!("Init");

        let client = Client::open(self.redis_connection_string.as_str()).unwrap();
        let mut conn = client.get_connection().unwrap();            // throw away the result, just make sure it does not fail

        let keys : Vec<String> = conn.hkeys(self.hash_name.to_string()).unwrap();
        let ts = time::now().to_timespec();

        println!("uid={}, gid={}", req.uid(), req.gid());

        let mut i = 2;
        for key in keys {
            let attr = HashAttr::new(FileType::RegularFile, i, ts, req.uid(), req.gid());
            self.inodes.insert(key.to_string(), attr.ino);
            self.attrs.insert(attr.ino, attr);
            i = i + 1;
        }
            
        let dir_attr = HashAttr::new(FileType::Directory, 1, ts, req.uid(), req.gid());

        self.attrs.insert(1, dir_attr);
        self.inodes.insert("/".to_string(), 1);

        //Err(libc::ENOENT)
        Ok(())
    }

    fn lookup(&mut self, req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        println!("Lookup");

        if parent == 1 {
            let inode = match self.inodes.get(name.to_str().unwrap()) {
                Some(inode) => inode,
                None => {
                    reply.error(ENOENT);
                    return;
                }
            };

            match self.attrs.get(inode) {
                Some(attr) => {
                    // A hack to change the ownership to current user
                    let mut file_attr = attr.clone();
                    file_attr.uid = req.uid();
                    file_attr.gid = req.gid();
                    // End 
                    let ttl = Timespec::new(1, 0);
                    //reply.entry(&ttl, attr, 0);
                    reply.entry(&ttl, &file_attr, 0);
                }
                None => reply.error(ENOENT),
            }
        }
    }

    fn getattr(&mut self, req: &Request, ino: u64, reply: ReplyAttr) {
        println!("getattr(ino={})", ino);
        println!("GETATTR uid={}, gid={}", req.uid(), req.gid());

        match self.attrs.get(&ino) {
            Some(attr) => {
                let ttl = Timespec::new(1, 0);
                reply.attr(&ttl, attr);
            }
            None => reply.error(ENOENT),
        };
    }

    fn read(&mut self, _req: &Request, ino: u64, _fh: u64, offset: i64, _size: u32, reply: ReplyData) {
        println!("read(ino={}, fh={}, offset={}, size={})", ino, _fh, offset, _size);

        for (key, &inode) in &self.inodes {
            println!("read(key={}, inode={}, ino={})", key, inode, ino);
            if inode == ino {
                println!("GOT read(key={}, inode={}, ino={})", key, inode, ino);
                let client = Client::open(self.redis_connection_string.as_str()).unwrap();
                let mut conn = client.get_connection().unwrap();// throw away the result, just make sure it does not fail

                let value : String = conn.hget(self.hash_name.to_string(), key).unwrap();

                reply.data(value.as_bytes());
                return;
            }
        }
        reply.error(ENOENT);
    }

    fn setattr(&mut self, _req: &Request, _ino: u64, _mode: Option<u32>, _uid: Option<u32>, _gid: Option<u32>, _size: Option<u64>, _atime: Option<Timespec>, _mtime: Option<Timespec>, _fh: Option<u64>, _crtime: Option<Timespec>, _chgtime: Option<Timespec>, _bkuptime: Option<Timespec>, _flags: Option<u32>, reply: ReplyAttr) {
        println!("setattr(ino={})", _ino);
        
        match self.attrs.get(&_ino) {
            Some(attr) => {
                let ttl = Timespec::new(1, 0);
                reply.attr(&ttl, attr);
            }
            None => reply.error(ENOENT),
        };
    }

    fn readdir(&mut self, _req: &Request, ino: u64, _fh: u64, offset: i64, mut reply: ReplyDirectory) {
        println!("readdir(ino={}, fh={}, offset={})", ino, _fh, offset);
        if ino == 1 {
            if offset == 0 {
                //reply.add(1, 0, FileType::Directory, ".");
                //reply.add(1, 1, FileType::Directory, "..");
                for (key, &inode) in &self.inodes {
                    if inode == 1 {
                        continue;
                    }
                    let offset = inode as i64; // hack
                    println!("readdir(ino={}, fh={}, offset={}, key={})", ino, _fh, offset, key);
                    reply.add(inode, offset, FileType::RegularFile, key);
                }
            }
            reply.ok();
        } else {
            reply.error(ENOENT);
        }
    }

    fn create(&mut self, req: &Request, _parent: u64, _name: &OsStr, _mode: u32, _flags: u32, reply: ReplyCreate) {
        println!("create(name={:?})", _name);

        let number_of_files = self.inodes.len();
        let ts = time::now().to_timespec();

        let attr = HashAttr::new(FileType::RegularFile, number_of_files as u64 + 2, ts, req.uid(), req.gid());

        println!("create INO(name={} and Number_Of_Files={})", attr.ino, number_of_files as u64 + 2);

        let name_of_file = _name.to_str().unwrap().to_string();

        self.inodes.insert(name_of_file, attr.ino);
        self.attrs.insert(attr.ino, attr);

        reply.created(&ts, &attr, 0, 0, 0);

    }

    fn write(&mut self, _req: &Request, _ino: u64, _fh: u64, _offset: i64, _data: &[u8], _flags: u32, reply: ReplyWrite){
        let string_data = String::from_utf8_lossy(_data);
        println!("write(data={:?})", string_data);

        for (key, &inode) in &self.inodes {
            if inode == _ino {
                println!("write(inode={:?})", _ino);  
                let client = Client::open(self.redis_connection_string.as_str()).unwrap();
                let mut conn = client.get_connection().unwrap();// throw away the result, just make sure it does not fail

                let _: () = conn.hset(self.hash_name.to_string(), key, string_data.to_string()).unwrap();
                reply.written(string_data.len() as u32);
                return;
            }
        }
        reply.error(ENOENT);
    }

    fn unlink(&mut self, _req: &Request, _parent: u64, _name: &OsStr, reply: ReplyEmpty) {
        println!("unlink(name={:?})", _name);

        let client = Client::open(self.redis_connection_string.as_str()).unwrap();
        let mut conn = client.get_connection().unwrap();// throw away the result, just make sure it does not fail

        let _: () = conn.hdel(self.hash_name.to_string(), _name.to_str().unwrap().to_string()).unwrap();

        let file_name = _name.to_str().unwrap();

        if !self.inodes.contains_key(file_name) {
            reply.error(ENOENT);
            return;            
        }

        let inode = self.inodes.get(file_name).cloned();

        self.inodes.remove(file_name);
        self.attrs.remove(&inode.unwrap());

        reply.ok();
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();

    let mountpoint = &args[1];
    let certs_hash = &args[2];
    let config_json_file = &args[3];

    let fs = HashFS::new(certs_hash.to_string(), config_json_file.to_string());
    fuse::mount(fs, &mountpoint, &[]).unwrap();
}
