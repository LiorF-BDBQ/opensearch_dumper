Usage:

```shell
docker build dumper:test .
docker run -v `pwd`:/data dumper:test dump split-products --hosts http://localhost:9200/ --max_slices 2
```