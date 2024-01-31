## Lab1: MapReduce
### 结果
![lab1_result.png](img%2Flab1_result.png)

### 1. 思路
![img_1.png](img%2Fimg_1.png)

### 2. 实现细节
1. 实现过程中没有使用互斥锁, 而是使用 $channel$ 来实现
2. 由于 $RPC$采用的是 $http$ 协议进行传输数据, 所以定义了$Request$ 请求体
和$Response$ 返回体


## Lab2: Raft
[raft论文中文翻译](https://zhuanlan.zhihu.com/p/343560811)

[很不错的raft实现过程和原理](https://www.cnblogs.com/brianleelxt/p/13251540.html)
### 选举过程
![img.png](img%2Fimg.png)

### 协程相关信息
![img_2.png](img%2Fimg_2.png)

### 大概设计
![img_3.png](img%2Fimg_3.png)


### 结果
#### 1. Lab2A
![lab2A_result.png](img%2Flab2A_result.png)

#### 2. Lab2B
![lab2B_result.png](img%2Flab2B_result.png)

#### 3. Lab2C
![lab2C_result.png](img%2Flab2C_result.png)

#### 4. Lba2D
![lab2D_result.png](img%2Flab2D_result.png)