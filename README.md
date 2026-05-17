# transfer

cross-platform file transfer over shared_memory

## how to use

```bash
cargo build --release
transfer -h
```

transfer a single file

```bash
# step1, run writer
transfer w -i /path/to/your/file

# step2, run reader
transfer r
```


transfer a whole directory

```bash
# step1, run writer
transfer w -i /path/to/your/directory

# step2, run reader
transfer r
```