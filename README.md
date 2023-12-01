### 说明

自用简易辅助索引,无第三方服务依赖，此服务主要功能是通过关键字key来搜索id<BR>
存储结构:<BR>
![architecture](https://gitee.com/yanbin_yb/pyfulltexts/raw/master/存储结构.png)


### 接口说明

### 关键字查询

#### 请求URL

- <span>https://localhost:8888/index</span>

#### 请求方式

- GET

#### 输入参数说明

|       参数名       | 必选 |  类型  |                       说明                       |
| :----------------: | :--: | :----: | :----------------------------------------------: |
|     target     |  是  | String |              数据存储地址               |
|       keys       |  否  | String | 查询关键字,多关键字逗号分格 |

#### 返回示例
```json
{"error_code":0,"data":[{"id":"12231","content":"这是一个示例","keys":["key1","key2"]}]}
```

###  创建更新索引

#### 接口描述

- 创建更新索引

#### 请求URL

- <span>https://localhost:8888/index</span>

#### 请求方式

- POST

#### 输入参数说明

|       参数名       | 必选 |  类型  |                       说明                       |
| :----------------: | :--: | :----: | :----------------------------------------------: |
|       id       |  是  | String | 唯一标识 |
|     target     |  是  | String |              数据存储地址               |
|       keys       |  否  | array | 查询关键字,多关键字逗号分格 |
|       content       |  否  | String | 内容 |


#### 系统按分词获取关键字调用示例
```json
{"target":"mytest","id":"1001","content":"可以使用db.session对象的query()方法来查询数据。"}
```

#### 自定义关键字调用示例
```json
{"target":"mytest","id":"1001","content":"可以使用db.session对象的query()方法来查询数据。","keys":["query","查询","数据"]}
```

#### 删除索引
```json
{"target":"mytest","id":"1001","keys":[]}
```

