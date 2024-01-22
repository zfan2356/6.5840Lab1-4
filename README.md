## Lab1: MapReduce
![img.png](img%2Fimg.png)

### 1. 思路
![img_1.png](img%2Fimg_1.png)

### 2. 实现细节
1. 实现过程中没有使用互斥锁, 而是使用channel来实现
3. 由于RPC采用的是http协议进行传输数据, 所以定义了Request请求体和Response返回体
