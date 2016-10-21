Looks like Luigu `JobRunner` class unable to handle secondary sort problem

I've tried job with following `jobconf`

```python
jcf.append("stream.num.map.output.key.fields=2")
jcf.append("mapred.text.key.partitioner.options=-k1,1")
jcf.append("mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator")
jcf.append("mapred.text.key.comparator.options=-k1,2")
```

TODO: find fix and PR error

Workaround: will do this part using plain streaming job