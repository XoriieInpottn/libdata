# libdata Documentation

## Access Resources with Unified URLs

`libdata` provides a **unified URL syntax** to access different types of resources (databases, file systems, remote services, etc.).
 This allows you to work with heterogeneous data sources using a single, consistent style.

------

### URL Syntax Rules

A standard URL usually consists of the following parts:

```
scheme://username:password@host:port/path?query#fragment
```

- **scheme**: Protocol or resource type, e.g. `mysql://`, `file://`, `http://`.
- **username:password@**: Optional. Used when authentication is required.
- **host:port**: Resource host and port.
- **path**: Resource path, which may represent a table, file path, or service endpoint.
- **query**: Extra parameters (optional).
- **fragment**: Rarely used in `libdata`.

> In `libdata`:
>
> - **scheme** decides which backend handler is used (MySQL, MongoDB, local file, S3, HTTP, etc.).
> - **path** is interpreted differently depending on the scheme.
> - **username/password** can be omitted; default or anonymous access will be used.
> - **port** can be omitted if the default port applies (e.g., MySQL = 3306, MongoDB = 27017, HTTP = 80/443).

------

### Databases

#### MySQL

```
mysql://username:password@host:port/database/table
```

- You can omit the `table` part and specify it later at runtime.

Example: [MySQL example code](https://github.com/XoriieInpottn/libdata/blob/main/example/mysql.py)

------

#### MongoDB

```
mongo://username:password@host:port/database/collection
mongodb://username:password@host:port/database/collection
```

- Both prefixes are supported.
- You can omit `collection` and choose one later.

------

#### Milvus

```
milvus://username:password@host:port/database/collection
```

- Unified URL format for Milvus vector databases.
- `database` and `collection` may also be specified later.

#### Redis

```
redis://username:password@host1:port1,host2:port2,host3:port3/database?service_name=xxx_service
```

- `username` is usually set as "default".
- The above URL is Sentinel mode, in which multiple host:port pairs are defined.
- `database` may leave empty or set as "0".

Example: [Redis example code](https://github.com/XoriieInpottn/libdata/blob/main/example/redis.py)

------

### File Systems

#### S3

```
s3+http://key:secret@host:port/path/of/dir/or/file
```

- Use `s3+http` or `s3+https` depending on the protocol.
- `path` can be a directory or a file.

Example: [S3 example code](https://github.com/XoriieInpottn/libdata/blob/main/example/fs.py)

------

#### Local Files and Directories

You can use several schemes for local files:

```
file:///path/of/dir/or/file
local:///path/of/dir/or/file
/path/of/dir/or/file
```

For structured data files, you can use specialized schemes:

```
json:///path/of/file
jsonl:///path/of/file
yaml:///path/of/file
yml:///path/of/file
jsondir:///path/of/dir
yamldir:///path/of/dir
ymldir:///path/of/dir
```

- `jsondir://` / `yamldir://`: Used for directories containing multiple JSON/YAML files, with file names as identifiers.

------

### Services

Remote services can be accessed using standard HTTP/HTTPS URLs:

```
http://host:port/path/of/service
https://host:port/path/of/service
```

- `libdata` wraps these as resources similar to databases or files.
- Supports common methods such as GET and POST.



# libdata 使用文档

## 使用 URL 来以统一的方式访问资源

`libdata` 通过 **统一的 URL 语法** 来访问不同类型的资源（数据库、文件系统、远程服务等）。
 这样，你只需要记住一种 URL 风格，就能操作多种异构数据源。

------

### URL 的使用规则

一个标准的 URL 通常由以下几个部分组成：

```
scheme://username:password@host:port/path?query#fragment
```

- **scheme**：协议或类型，用来区分资源类别。例如 `mysql://`、`file://`、`http://`。
- **username:password@**：可选，用于需要身份验证的资源。
- **host:port**：资源所在的地址与端口。
- **path**：资源的路径，可能是数据库的表名、文件路径或服务路径。
- **query**：额外参数，可用于传递选项（目前可选）。
- **fragment**：锚点（很少使用）。

> 在 `libdata` 中：
>
> - **scheme** 决定了解析器（MySQL、MongoDB、本地文件、S3、HTTP 服务等）。
> - **path** 部分的解释会因 scheme 不同而有所变化。
> - **用户名/密码** 如果省略，会尝试匿名或默认配置。
> - **端口** 如果省略，则使用资源的默认端口（如 MySQL = 3306，MongoDB = 27017，HTTP = 80/443）。

------

### 数据库类

#### MySQL

```
mysql://username:password@host:port/database/table
```

- 可以只写到 `database`，在使用时再指定表。

使用例子：[MySQL 示例代码](https://github.com/XoriieInpottn/libdata/blob/main/example/mysql.py)

------

#### MongoDB

```
mongo://username:password@host:port/database/collection
mongodb://username:password@host:port/database/collection
```

- 两种写法都支持。
- 也可以只写到 `database`，等到操作时再指定集合。

------

#### Milvus

```
milvus://username:password@host:port/database/collection
```

- 用于向量数据库 Milvus 的统一 URL。
- `database` 和 `collection` 可在后续操作时指定。

------

#### Redis

```
redis://username:password@host1:port1,host2:port2,host3:port3/database?service_name=xxx_service
```

- `username` 通常设为 `"default"`。
- 上述 URL 表示 **哨兵模式 (Sentinel mode)**，其中可以定义多个 `host:port` 对。
- `database` 可以留空，或设为 `"0"`。

使用例子：[Redis 示例代码](https://github.com/XoriieInpottn/libdata/blob/main/example/redis.py)

------

### 文件系统类

#### S3 文件系统

```
s3+http://key:secret@host:port/path/of/dir/or/file
```

- 使用 `s3+http` 或 `s3+https` 来区分访问协议。
- `path` 可以是文件或目录。

使用例子：[S3 文件系统示例代码](https://github.com/XoriieInpottn/libdata/blob/main/example/fs.py)

------

#### 本地文件/目录

本地文件系统可以通过多种 scheme 来区分文件格式：

```
file:///path/of/dir/or/file
local:///path/of/dir/or/file
/path/of/dir/or/file
```

如果是结构化数据文件，还可以用专门的 scheme：

```
json:///path/of/file
jsonl:///path/of/file
yaml:///path/of/file
yml:///path/of/file
jsondir:///path/of/dir
yamldir:///path/of/dir
ymldir:///path/of/dir
```

- `jsondir://` / `yamldir://`：用于存放多个 JSON/YAML 文件的目录，按文件名区分对象。

------

### 服务类

远程 HTTP/HTTPS 服务可以直接用标准 URL：

```
http://host:port/path/of/service
https://host:port/path/of/service
```

- `libdata` 会将其封装为类似文件/数据库资源的接口。
- 支持 GET/POST 等常见方法。

