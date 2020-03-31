# redis-backed-filesystem-in-rust
This is for learning purpose. Not to be used in Production

# Build instructions
$ rustup default nightly # Switch to nightly

$ ./target/debug/hashfs ~/mount kb_hash ./src/config.json # Run it with arguements as <mount_point> <redis_hash> <config_file> 