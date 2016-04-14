自己测试Azkaban3.0.0和hdfs viewer插件的时候发现版本不匹配导致无法使用hdfs viewer插件，因此重新将hdfs viewer的功能整合到Azkaban web server中。


Azkaban 3 [![Build Status](http://img.shields.io/travis/azkaban/azkaban.svg?style=flat)](https://travis-ci.org/azkaban/azkaban)
========

Building from Source
--------------------

To build Azkaban packages from source, run:

```
./gradlew distTar
```

The above command builds all Azkaban packages and packages them into GZipped Tar archives. To build Zip archives, run:

```
./gradlew distZip
```

Documentation
-------------

For Azkaban documentation, please go to [Azkaban Project Site](http://azkaban.github.io). The source code for the documentation is in the [gh-pages branch](https://github.com/azkaban/azkaban/tree/gh-pages).

For help, please visit the Azkaban Google Group: [Azkaban Group](https://groups.google.com/forum/?fromgroups#!forum/azkaban-dev)
