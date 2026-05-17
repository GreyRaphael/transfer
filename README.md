# transfer

cross-platform file transfer over shared_memory

## how to use versioon 1

```bash
cargo build --release
transfer -h
```

> transfer a single file

```bash
# step1, run writer
transfer w -i /path/to/your/file

# step2, run reader
transfer r
```

> transfer a whole directory

```bash
# step1, run writer
transfer w -i /path/to/your/directory

# step2, run reader
transfer r
```

## how to use version 2

> data can only from writer to reader

```bash
# reader monitor current directory
transfer sync-r

# writer monitory /directory/to/monitor
transfer sync-w -i /directory/to/monitor
```

## how to trigger release

```bash
# 先push source
git push
# 再push tag
git tag v1.0.0
git push origin v1.0.0
```