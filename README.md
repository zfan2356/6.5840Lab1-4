## Lab1: MapReduce
![img.png](img%2Fimg.png)

### 1. 思路
![img_1.png](img%2Fimg_1.png)

### 2. 实现细节
1. 实现过程中没有使用互斥锁, 而是使用 $channel$ 来实现
2. 由于 $RPC$采用的是 $http$ 协议进行传输数据, 所以定义了$Request$ 请求体
和$Response$ 返回体